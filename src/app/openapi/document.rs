//! The document itself: the operations a route contributes, and the two passes that turn the
//! derived schemas into what a caller actually sends.

use utoipa::openapi::path::{Operation, OperationBuilder};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::{
    Components, ContentBuilder, HttpMethod, InfoBuilder, OpenApi, OpenApiBuilder, Paths, RefOr,
    ResponseBuilder, ResponsesBuilder, Schema,
};

use crate::app::openapi::page::{nullable, referenced};

/// What every operation's error responses say. The statuses are the same on every route
/// because the refusals are: a body this service cannot read as written is a 400 wherever it
/// arrived.
fn responses(ok: &str, schema: RefOr<Schema>) -> utoipa::openapi::Responses {
    let json = |schema: RefOr<Schema>| ContentBuilder::new().schema(Some(schema)).build();
    ResponsesBuilder::new()
        .response(
            "200",
            ResponseBuilder::new()
                .description(ok)
                .content("application/json", json(schema)),
        )
        .response(
            "400",
            ResponseBuilder::new().description(
                "The request could not be read as written: a field this route has no use for, \
                 an expression that does not parse, a column the file has not got, or a url \
                 the access policy refuses.",
            ),
        )
        .response(
            "502",
            ResponseBuilder::new()
                .description("The store the url named could not be reached or read."),
        )
        .build()
}

/// One operation, with its body and its answer.
pub(in crate::app) fn operation(
    tag: &str,
    summary: &str,
    description: &str,
    body: RefOr<Schema>,
    example: serde_json::Value,
    ok: &str,
    answer: RefOr<Schema>,
) -> Operation {
    OperationBuilder::new()
        .tag(tag)
        .summary(Some(summary))
        .description(Some(description))
        .request_body(Some(
            RequestBodyBuilder::new()
                .content(
                    "application/json",
                    ContentBuilder::new()
                        .schema(Some(body))
                        .example(Some(example))
                        .build(),
                )
                .required(Some(utoipa::openapi::Required::True))
                .build(),
        ))
        .responses(responses(ok, answer))
        .build()
}

/// A `POST` at one path.
pub(in crate::app) fn post(paths: &mut Paths, path: &str, operation: Operation) {
    paths.add_path_operation(path, vec![HttpMethod::Post], operation);
}

/// Collapse every component that is an `allOf` of other components into one flat object, and
/// drop the parts they were made of.
///
/// A `#[serde(flatten)]` is a Rust arrangement — `StorageOptions` groups its fields per backend
/// so that a backend function cannot reach another's — and the wire form stays one flat object.
/// The derived schema records each as `allOf`, which would show a caller nested objects a body
/// has none of, and show a `$ref` in place of the fields it stands for. What a caller sends is
/// one flat set of keys, so that is what is described.
///
/// A part is dropped only after every component has been flattened, since two components may
/// absorb the same part, and removing it for the first would leave the second describing a
/// `$ref` to nothing.
fn flatten_components(components: &mut Components) {
    // Names first: flattening rewrites the map.
    let names = components.schemas.keys().cloned().collect::<Vec<_>>();
    let absorbed = names
        .iter()
        .flat_map(|name| flatten_component(components, name))
        .collect::<Vec<_>>();
    for part in absorbed {
        components.schemas.remove(&part);
    }
}

/// One such component, flattened in place, and the parts it absorbed.
fn flatten_component(components: &mut Components, name: &str) -> Vec<String> {
    let Some(RefOr::T(Schema::AllOf(all_of))) = components.schemas.get(name).cloned() else {
        return Vec::new();
    };
    let mut flat = utoipa::openapi::Object::new();
    let mut absorbed = Vec::new();
    for part in &all_of.items {
        // Each part is either the struct's own fields, inline, or a `$ref` to a group.
        let object = match part {
            RefOr::T(Schema::Object(object)) => Some(object.clone()),
            RefOr::Ref(_) => referenced(part).and_then(|part| {
                absorbed.push(part.to_owned());
                match components.schemas.get(part) {
                    Some(RefOr::T(Schema::Object(object))) => Some(object.clone()),
                    _ => None,
                }
            }),
            _ => None,
        };
        let Some(object) = object else { continue };
        for (property, schema) in object.properties {
            flat.properties.insert(property, schema);
        }
        flat.required.extend(object.required);
    }
    flat.description = all_of.description;
    components.schemas.insert(name.to_owned(), flat.into());
    absorbed
}

/// Say which url schemes each storage option applies to, on the option itself.
///
/// Written into the description rather than left to prose someone keeps in step: the list
/// comes from [`crate::storage::option_schemes`], which is the same one the service refuses
/// by, so the document cannot claim an option is for a backend that would reject it.
///
/// The options are one flat set of fields for every backend — the url's scheme is what says
/// which apply — so without this a reader sees fifteen optional fields and no way to tell that
/// `sas_token` is Azure's and `region` is S3's.
fn note_which_backend(components: &mut Components) {
    let Some(RefOr::T(Schema::Object(options))) = components.schemas.get_mut("StorageOptions")
    else {
        return;
    };
    for (name, property) in &mut options.properties {
        let schemes = crate::storage::option_schemes(name);
        if schemes.is_empty() {
            continue;
        }
        let note = format!("For {} urls.", schemes.join(", "));
        // An optional field whose type is another component arrives as `oneOf[null, $ref]`,
        // with the description on the reference inside. Reaching through that is what keeps
        // `transport` from being the one option with nothing said about it.
        let wrapped = nullable(property).is_some();
        let target = match (wrapped, property) {
            (true, RefOr::T(Schema::OneOf(one_of))) => one_of.items.iter_mut().find(|item| {
                !matches!(item, RefOr::T(Schema::Object(object))
                    if matches!(&object.schema_type,
                        utoipa::openapi::schema::SchemaType::Type(
                            utoipa::openapi::schema::Type::Null)))
            }),
            (_, other) => Some(other),
        };
        let description = match target {
            Some(RefOr::T(Schema::Object(object))) => &mut object.description,
            Some(RefOr::Ref(reference)) => {
                reference.description = match reference.description.is_empty() {
                    true => note,
                    false => format!("{note} {}", reference.description),
                };
                continue;
            }
            _ => continue,
        };
        *description = Some(match description.take() {
            Some(text) => format!("{note} {text}"),
            None => note,
        });
    }
}

/// The document around whatever paths and components the routes contributed.
pub(in crate::app) fn document(paths: Paths, mut components: Components) -> OpenApi {
    flatten_components(&mut components);
    note_which_backend(&mut components);
    OpenApiBuilder::new()
        .info(
            InfoBuilder::new()
                .title("hats-api")
                .version(env!("CARGO_PKG_VERSION"))
                .description(Some(
                    "Query HATS catalogs and parquet files over HTTP: a region of the sky, a \
                     row predicate and the columns you want, answered as JSON, parquet or a \
                     VOTable — or as a plan, one request per partition, where the work is too \
                     large for one answer.\n\n\
                     Every query route is a `POST`: the body carries a url and, where the \
                     store needs them, the caller's own credentials — which a query string \
                     would write into every proxy's access log on the way. A body also has \
                     no url-length limit, which a long column list reaches.\n\n\
                     The last path segment names what the request is against. Each route \
                     takes its own body, listed below it, and a key that is not one of its \
                     fields is refused rather than ignored.\n\n\
                     This describes the API. A deployment may also serve directories of \
                     files, where every url below the mount is a data path and there is no \
                     route set to enumerate; the README covers that half.",
                ))
                .build(),
        )
        .paths(paths)
        .components(Some(components))
        .build()
}

/// The health route, which is the one operation that queries nothing.
pub(in crate::app) fn health(paths: &mut Paths, path: &str) {
    let operation = OperationBuilder::new()
        .tag("service")
        .summary(Some("Whether the service is up"))
        .description(Some(
            "Answers without opening a store or reading a file, so it says the process is \
             running and nothing about whether any url is reachable.",
        ))
        .responses(
            ResponsesBuilder::new()
                .response(
                    "200",
                    ResponseBuilder::new()
                        .description("The service is up")
                        .content(
                            "application/json",
                            ContentBuilder::new()
                                .schema(Some(crate::app::health_schema()))
                                .build(),
                        ),
                )
                .build(),
        )
        .build();
    paths.add_path_operation(path, vec![HttpMethod::Get], operation);
}
