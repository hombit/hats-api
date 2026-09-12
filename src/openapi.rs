//! The API's own description, and a page that renders it.
//!
//! The schemas come from the same types the routes deserialize, so a field added to a
//! request appears here by compiling rather than by being remembered. The paths do not: they
//! are built from the same loop that registers the routes ([`crate::app::router`]), which is
//! what keeps a vocabulary from being served and undescribed.
//!
//! It describes API mode only. The file-server mode has no route set to enumerate — every
//! url below a mount is a data path — so OpenAPI would have to invent a shape for it.

use utoipa::openapi::path::{Operation, OperationBuilder};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::{
    Components, ContentBuilder, HttpMethod, InfoBuilder, OpenApi, OpenApiBuilder, Paths, RefOr,
    ResponseBuilder, ResponsesBuilder, Schema,
};

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
pub(crate) fn operation(
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
pub(crate) fn post(paths: &mut Paths, path: &str, operation: Operation) {
    paths.add_path_operation(path, vec![HttpMethod::Post], operation);
}

/// Collapse every component that is an `allOf` of other components into one flat object, and
/// drop the parts they were made of.
///
/// A `#[serde(flatten)]` is a Rust arrangement — `StorageOptions` groups its fields per backend
/// so that a backend function cannot reach another's, a request body holds its vocabulary that
/// way, and the plan body holds the whole catalog query that way — and the wire form stays one
/// flat object. The derived schema records each as `allOf`, which would show a caller nested
/// objects a body has none of, and show a `$ref` in place of the fields it stands for. What a
/// caller sends is one flat set of keys, so that is what is described.
///
/// A part is dropped only after every component has been flattened, since two of them absorb
/// the same vocabulary — a request body and a plan entry's body both flatten it — and removing
/// it for the first would leave the second describing a `$ref` to nothing.
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

/// Put one component's fields in the order given, and anything the order does not name after
/// them, in whatever order it was in.
///
/// The order a body is written in is not one this module can work out. A `#[serde(flatten)]`
/// is an `allOf` in the schema and the flattened part always comes first, so a request's
/// projection would sit above the `url` it is a projection of — and no rule over "required"
/// and "not" recovers it either. So the endpoint that owns the fields states the order, and
/// this applies it.
///
/// A field the order does not name still appears: a description that omitted a field would be
/// wrong in a way a caller acts on, while one that lists it last is only untidy.
pub(crate) fn order_fields(document: &mut OpenApi, component: &str, order: &[&str]) {
    let Some(components) = document.components.as_mut() else {
        return;
    };
    let Some(RefOr::T(Schema::Object(object))) = components.schemas.get_mut(component) else {
        return;
    };
    let mut fields = std::mem::take(&mut object.properties)
        .into_iter()
        .collect::<Vec<_>>();
    let rank = |name: &String| {
        order
            .iter()
            .position(|field| field == name)
            .unwrap_or(order.len())
    };
    fields.sort_by_key(|(name, _)| rank(name));
    object.properties.extend(fields);
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
pub(crate) fn document(paths: Paths, mut components: Components) -> OpenApi {
    flatten_components(&mut components);
    note_which_backend(&mut components);
    OpenApiBuilder::new()
        .info(
            InfoBuilder::new()
                .title("hats-api")
                .version(env!("CARGO_PKG_VERSION"))
                .description(Some(
                    "A read-only query service over parquet files and HATS catalogs.\n\n\
                     Every query route is a `POST`: the body carries a url and, where the \
                     store needs them, the caller's own credentials — which a query string \
                     would write into every proxy's access log on the way. A body also has \
                     no url-length limit, which a long select list reaches.\n\n\
                     The first path segment is the vocabulary the body is written in — both \
                     say the same thing and lower to the same plan, differing in what a field \
                     may hold — and the rest names what the request is against. Each route \
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

/// The health route, which is the one operation belonging to no vocabulary.
pub(crate) fn health(paths: &mut Paths, path: &str) {
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

/// The page that renders the document.
///
/// Rendered here rather than fetched and drawn by a script, for the reason every page this
/// service serves is: the markup is complete without JavaScript, nothing is fetched from a
/// network the browser may not reach, and it is the same hand as a directory listing. The
/// only interactive part is `<details>`, which is the browser's.
///
/// It renders *this* document rather than arbitrary OpenAPI — enough of the vocabulary for
/// what `app::describe` emits, and no schema keyword nothing here produces.
pub(crate) fn page(document: &OpenApi, document_url: &str) -> String {
    let mut html = String::new();
    let info = &document.info;
    html.push_str(&format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{title}</title>\n<style>\n{css}</style>\n</head>\n<body>\n\
         <h1>{title}<span class=\"version\">{version}</span></h1>\n",
        title = escape(&info.title),
        version = escape(&info.version),
        css = include_str!("openapi/page.css"),
    ));
    for paragraph in info.description.as_deref().unwrap_or("").split("\n\n") {
        html.push_str(&format!("<p class=\"lede\">{}</p>\n", escape(paragraph)));
    }
    html.push_str(&format!(
        "<p>The same description as JSON, for a client generator: \
         <a href=\"{url}\"><code>{url}</code></a></p>\n",
        url = escape_attr(document_url),
    ));

    html.push_str(&contents(document));

    for (tag, operations) in grouped(document) {
        html.push_str(&format!(
            "<section class=\"group\" id=\"{id}\">\n<h2>{tag} <a class=\"anchor\" \
             href=\"#{id}\">#</a></h2>\n",
            id = escape_attr(&group_anchor(&tag)),
            tag = escape(&tag),
        ));
        // The vocabulary's own line, once above its routes rather than repeated in each: it
        // is the same sentence three times, and three of them read as three facts.
        if let Some(note) = operations
            .first()
            .and_then(|(_, _, op)| op.description.as_deref())
        {
            html.push_str(&format!("<p class=\"group-note\">{}</p>\n", escape(note)));
        }
        for (method, path, operation) in operations {
            html.push_str(&render_operation(&method, &path, operation, document));
        }
        html.push_str("</section>\n");
    }

    html.push_str("<section class=\"types\" id=\"types\">\n<h2>Types</h2>\n");
    for (name, schema) in components(document) {
        html.push_str(&format!(
            "<article class=\"type\" id=\"{id}\">\n\
             <h3>{name} <a class=\"anchor\" href=\"#{id}\">#</a></h3>\n",
            id = escape_attr(&anchor(&name)),
            name = escape(&name),
        ));
        if let Some(note) = describe(schema) {
            html.push_str(&format!("<p class=\"type-note\">{}</p>\n", escape(note)));
        }
        html.push_str(&render_schema(schema));
        html.push_str("</article>\n");
    }
    html.push_str("</section>\n");

    html.push_str(&format!(
        "<p class=\"footer\">This describes the API. Where the deployment also serves \
         directories, every url below a mount is a data path and has no route set to \
         enumerate.</p>\n<script>\n{script}</script>\n</body>\n</html>\n",
        script = include_str!("openapi/page.js"),
    ));
    html
}

fn escape(text: &str) -> String {
    html_escape::encode_text(text).into_owned()
}

fn escape_attr(text: &str) -> String {
    html_escape::encode_double_quoted_attribute(text).into_owned()
}

/// A component's name as a fragment, which is also how a `$ref` to it is linked.
fn anchor(name: &str) -> String {
    format!("type-{name}")
}

fn group_anchor(tag: &str) -> String {
    format!("group-{tag}")
}

/// One route's fragment, so a route can be linked to and so the contents can reach it.
///
/// Built from the method and the path rather than from the summary: the summary is prose and
/// would change under someone editing the description, taking every link to it with it.
fn route_anchor(method: &str, path: &str) -> String {
    let spelled: String = path
        .chars()
        .map(|character| match character.is_ascii_alphanumeric() {
            true => character,
            false => '-',
        })
        .collect();
    format!("{}{spelled}", method.to_lowercase())
}

/// What is on the page, before the page itself.
///
/// Seven routes and sixteen types is already more than fits on a screen, and the routes are
/// collapsed — so without this the only way to find one is to scroll reading summaries. Plain
/// links, so it works with the script off like everything else here.
fn contents(document: &OpenApi) -> String {
    let mut html = String::from("<nav class=\"contents\">\n");
    for (tag, operations) in grouped(document) {
        html.push_str(&format!(
            "<div class=\"toc-group\">\n<h3><a href=\"#{id}\">{tag}</a></h3>\n<ul>\n",
            id = escape_attr(&group_anchor(&tag)),
            tag = escape(&tag),
        ));
        for (method, path, operation) in operations {
            html.push_str(&format!(
                "<li><a href=\"#{id}\"><span class=\"toc-method\">{method}</span>\
                 <code>{path}</code></a> <span class=\"toc-summary\">{summary}</span></li>\n",
                id = escape_attr(&route_anchor(&method, &path)),
                method = escape(&method),
                path = escape(&path),
                summary = escape(operation.summary.as_deref().unwrap_or("")),
            ));
        }
        html.push_str("</ul>\n</div>\n");
    }
    html.push_str("<div class=\"toc-group\">\n<h3><a href=\"#types\">types</a></h3>\n<ul class=\"toc-types\">\n");
    for (name, _) in components(document) {
        html.push_str(&format!(
            "<li><a href=\"#{id}\"><code>{name}</code></a></li>\n",
            id = escape_attr(&anchor(&name)),
            name = escape(&name),
        ));
    }
    html.push_str("</ul>\n</div>\n</nav>\n");
    html
}

/// The operations by tag, each group in the order the paths were registered.
///
/// A `BTreeMap` would put the vocabularies in alphabetical order, which is an order nobody
/// chose; registration order is the one the routes are declared in.
type Operations<'a> = Vec<(String, String, &'a Operation)>;

fn grouped(document: &OpenApi) -> Vec<(String, Operations<'_>)> {
    let mut groups: Vec<(String, Operations<'_>)> = Vec::new();
    for (path, item) in &document.paths.paths {
        // A `PathItem` is a field per method rather than a map, so the two this service uses
        // are named. A method added to a route and not to this list would be missing from the
        // page, which is why they are listed together rather than looked up where used.
        let methods = [("GET", &item.get), ("POST", &item.post)];
        for (method, operation) in methods {
            let Some(operation) = operation else { continue };
            let tag = operation
                .tags
                .as_ref()
                .and_then(|tags| tags.first())
                .cloned()
                .unwrap_or_else(|| "other".to_owned());
            let entry = (method.to_owned(), path.clone(), operation);
            match groups.iter_mut().find(|(name, _)| *name == tag) {
                Some((_, operations)) => operations.push(entry),
                None => groups.push((tag, vec![entry])),
            }
        }
    }
    groups
}

/// The named schemas, in the order they were registered.
fn components(document: &OpenApi) -> Vec<(String, &RefOr<Schema>)> {
    document
        .components
        .as_ref()
        .map(|components| {
            components
                .schemas
                .iter()
                .map(|(name, schema)| (name.clone(), schema))
                .collect()
        })
        .unwrap_or_default()
}

/// A schema's own prose, whichever kind of schema it is.
fn describe(schema: &RefOr<Schema>) -> Option<&str> {
    match schema {
        RefOr::Ref(reference) => {
            Some(reference.description.as_str()).filter(|text| !text.is_empty())
        }
        RefOr::T(Schema::Object(object)) => object.description.as_deref(),
        RefOr::T(Schema::Array(array)) => array.description.as_deref(),
        // An optional field whose type is a named component is `oneOf[null, $ref]`, and what
        // was said about it is on the reference inside rather than on the wrapper.
        RefOr::T(Schema::OneOf(one_of)) => one_of
            .description
            .as_deref()
            .or_else(|| nullable(schema).and_then(describe)),
        RefOr::T(Schema::AllOf(all_of)) => all_of.description.as_deref(),
        RefOr::T(Schema::AnyOf(any_of)) => any_of.description.as_deref(),
        _ => None,
    }
}

/// The component a `$ref` names, or `None` for anything else — including a `$ref` pointing
/// outside `#/components/schemas/`, which this document does not produce.
fn referenced(schema: &RefOr<Schema>) -> Option<&str> {
    match schema {
        RefOr::Ref(reference) => reference.ref_location.strip_prefix("#/components/schemas/"),
        RefOr::T(_) => None,
    }
}

/// One route: the line that stays visible, and the body and answers behind it.
fn render_operation(method: &str, path: &str, operation: &Operation, document: &OpenApi) -> String {
    let mut html = format!(
        "<details class=\"op\" id=\"{id}\">\n<summary><span class=\"method\">{method}</span>\
         <span class=\"path\">{path}</span><span class=\"summary\">{summary}</span></summary>\n\
         <div class=\"op-body\">\n",
        id = escape_attr(&route_anchor(method, path)),
        method = escape(method),
        path = escape(path),
        summary = escape(operation.summary.as_deref().unwrap_or("")),
    );

    if let Some(body) = &operation.request_body
        && let Some(content) = body.content.get("application/json")
        && let Some(schema) = &content.schema
    {
        html.push_str("<h4>request body</h4>\n");
        if let Some(name) = referenced(schema) {
            html.push_str(&render_named(name, document));
        } else {
            html.push_str(&render_schema(schema));
        }
    }

    html.push_str("<h4>answers</h4>\n<table class=\"status\">\n");
    for (status, response) in &operation.responses.responses {
        let RefOr::T(response) = response else {
            continue;
        };
        let schema = response
            .content
            .get("application/json")
            .and_then(|content| content.schema.as_ref())
            .and_then(referenced);
        html.push_str(&format!(
            "<tr><td>{status}</td><td class=\"about\">{description}{shape}</td></tr>\n",
            status = escape(status),
            description = escape(&response.description),
            shape = match schema {
                Some(name) => format!(
                    " — <a href=\"#{id}\"><code>{name}</code></a>",
                    id = escape_attr(&anchor(name)),
                    name = escape(name)
                ),
                None => String::new(),
            },
        ));
    }
    html.push_str("</table>\n");
    if method == "POST" {
        html.push_str(&render_runner(path, operation));
    }
    html.push_str("</div>\n</details>\n");
    html
}

/// The panel that sends the request, and the same request as `curl`.
///
/// Both, because they are for different people. The button answers "what does this return"
/// without leaving the page; the `curl` line is what anyone whose request needs a credential
/// should use instead, since a secret typed into a browser form is one in the page, in the
/// session's memory and in whatever the browser decides to keep.
///
/// The body is prefilled from the schema's own examples, so the form starts as a request that
/// is at least well-formed. Nothing is stored: no history, no last-used values.
fn render_runner(path: &str, operation: &Operation) -> String {
    // The route's own example, not one assembled from whatever fields carry one. Which fields
    // a route accepts is the route's to know, and an example is a body that has to run: one
    // built by collecting every example in the schema would start the form off with a request
    // this route cannot answer.
    let body = operation
        .request_body
        .as_ref()
        .and_then(|body| body.content.get("application/json"))
        .and_then(|content| content.example.as_ref())
        .map_or_else(
            || "{}".to_owned(),
            |example| serde_json::to_string_pretty(example).unwrap_or_else(|_| "{}".to_owned()),
        );
    format!(
        "<h4>try it</h4>\n\
         <form class=\"try\" data-path=\"{path_attr}\">\n\
         <textarea class=\"body\" rows=\"{rows}\" spellcheck=\"false\" \
         aria-label=\"request body\">{body}</textarea>\n\
         <div class=\"try-actions\">\n\
         <button class=\"with-js send\" type=\"submit\">Send</button>\n\
         <span class=\"note\">Sent from this page, to this service. A request needing a \
         credential belongs in the shell below, not in a browser form.</span>\n\
         </div>\n\
         <pre class=\"answer\" hidden></pre>\n\
         <p class=\"as-curl-label\">The same request as <code>curl</code>:</p>\n\
         <pre class=\"as-curl\">{curl}</pre>\n\
         </form>\n",
        path_attr = escape_attr(path),
        rows = body.lines().count().clamp(3, 18),
        body = escape(&body),
        curl = escape(&curl(path, &body)),
    )
}

/// The request as a shell command, with the origin left for the page to fill in — the server
/// knows the path it serves and not the name the browser reached it by.
fn curl(path: &str, body: &str) -> String {
    format!(
        "curl -sS -X POST '{path}' \\\n  -H 'content-type: application/json' \\\n  -d '{body}'",
        body = body.replace('\'', r"'\''"),
    )
}

/// A `$ref`'d body, rendered where it is used: the fields, plus a link to the whole type.
fn render_named(name: &str, document: &OpenApi) -> String {
    let Some((_, schema)) = components(document)
        .into_iter()
        .find(|(component, _)| component == name)
    else {
        return String::new();
    };
    format!(
        "{table}<p class=\"inherits\">The whole of \
         <a href=\"#{id}\"><code>{name}</code></a>.</p>\n",
        table = render_schema(schema),
        id = escape_attr(&anchor(name)),
        name = escape(name),
    )
}

/// A schema as a table of what a caller may write.
fn render_schema(schema: &RefOr<Schema>) -> String {
    match schema {
        RefOr::Ref(_) => referenced(schema).map_or_else(String::new, |name| {
            format!(
                "<p class=\"inherits\">As <a href=\"#{id}\"><code>{name}</code></a>.</p>\n",
                id = escape_attr(&anchor(name)),
                name = escape(name)
            )
        }),
        RefOr::T(Schema::Object(object)) => render_object(object),
        // A composite of a `$ref` and an object is a `flatten`: the fields of both, which is
        // what a caller writes as one flat body. Rendered as both rather than as a link,
        // since the fields arrive together and a reader is looking at one request.
        RefOr::T(Schema::AllOf(all_of)) => all_of.items.iter().map(render_schema).collect(),
        RefOr::T(Schema::OneOf(one_of)) => one_of
            .items
            .iter()
            .map(|item| {
                let tag = tag_of(item);
                let heading = match &tag {
                    Some((name, value)) => format!("{name}: {value}"),
                    None => "one of".to_owned(),
                };
                // The tag is not listed among the fields: the heading is the tag, so a row for
                // it would say the same thing again, with nothing in the column that explains
                // fields — it is the one property whose value the shape, not the caller, fixes.
                let table = match (&tag, item) {
                    (Some((name, _)), RefOr::T(Schema::Object(object))) => {
                        render_object_without(object, Some(name.as_str()))
                    }
                    _ => render_schema(item),
                };
                format!(
                    "<div class=\"variant\">\n<h5>{}</h5>\n{table}</div>\n",
                    escape(&heading)
                )
            })
            .collect(),
        _ => String::new(),
    }
}

/// The tag a `oneOf` variant carries, as the name of the property and the one value it may
/// take — which is how `serde`'s `tag = "type"` lands in a schema.
fn tag_of(schema: &RefOr<Schema>) -> Option<(String, String)> {
    let RefOr::T(Schema::Object(object)) = schema else {
        return None;
    };
    object.properties.iter().find_map(|(name, property)| {
        let RefOr::T(Schema::Object(property)) = property else {
            return None;
        };
        let [only] = property.enum_values.as_ref()?.as_slice() else {
            return None;
        };
        let spelled = only
            .as_str()
            .map_or_else(|| only.to_string(), str::to_owned);
        Some((name.clone(), spelled))
    })
}

/// One object's properties, in the order the type declares them.
fn render_object(object: &utoipa::openapi::Object) -> String {
    render_object_without(object, None)
}

fn render_object_without(object: &utoipa::openapi::Object, skip: Option<&str>) -> String {
    if object
        .properties
        .keys()
        .all(|name| Some(name.as_str()) == skip)
    {
        return String::new();
    }
    let mut html = String::from(
        "<table class=\"schema\">\n<thead><tr><th>field</th><th>type</th><th>what it is</th>\
         </tr></thead>\n<tbody>\n",
    );
    for (name, property) in &object.properties {
        if Some(name.as_str()) == skip {
            continue;
        }
        let required = object.required.iter().any(|field| field == name);
        html.push_str(&format!(
            "<tr><td class=\"name{class}\">{name}</td><td class=\"type\">{kind}</td>\
             <td class=\"about\">{about}</td></tr>\n",
            class = if required { " req" } else { "" },
            name = escape(name),
            kind = kind_of(property),
            about = escape(describe(property).unwrap_or("")),
        ));
    }
    html.push_str("</tbody>\n</table>\n");
    html
}

/// The one meaningful member of a `oneOf[null, T]`, which is how an optional field whose type
/// is a named component arrives.
///
/// A wrapper rather than a choice a caller makes: `null` is what leaving the field out means,
/// which the table already says by not marking it required. Reporting the wrapper instead of
/// `T` tells a reader "one of" and leaves them to guess one of what.
fn nullable(schema: &RefOr<Schema>) -> Option<&RefOr<Schema>> {
    let RefOr::T(Schema::OneOf(one_of)) = schema else {
        return None;
    };
    let is_null = |item: &&RefOr<Schema>| {
        matches!(item, RefOr::T(Schema::Object(object))
            if matches!(&object.schema_type,
                utoipa::openapi::schema::SchemaType::Type(utoipa::openapi::schema::Type::Null)))
    };
    let mut members = one_of.items.iter().filter(|item| !is_null(item));
    let only = members.next()?;
    members.next().is_none().then_some(only)
}

/// The type column: a link where the value is one of this document's own types, and the
/// JSON type otherwise.
fn kind_of(schema: &RefOr<Schema>) -> String {
    if let Some(inner) = nullable(schema) {
        return kind_of(inner);
    }
    if let Some(name) = referenced(schema) {
        return format!(
            "<a href=\"#{id}\">{name}</a>",
            id = escape_attr(&anchor(name)),
            name = escape(name)
        );
    }
    match schema {
        RefOr::T(Schema::Array(array)) => match &array.items {
            utoipa::openapi::schema::ArrayItems::RefOrSchema(items) => {
                format!("{}[]", kind_of(items))
            }
            utoipa::openapi::schema::ArrayItems::False => "[]".to_owned(),
        },
        RefOr::T(Schema::Object(object)) => escape(&spell(&object.schema_type)),
        RefOr::T(Schema::OneOf(_)) => "one of".to_owned(),
        RefOr::T(Schema::AllOf(_)) | RefOr::T(Schema::AnyOf(_)) => "object".to_owned(),
        _ => String::new(),
    }
}

/// A JSON type as a caller would say it. `null` is dropped from a union: every optional field
/// carries it, so printing it says only that the field is optional, which the table already
/// says by not marking it required.
fn spell(schema_type: &utoipa::openapi::schema::SchemaType) -> String {
    use utoipa::openapi::schema::{SchemaType, Type};
    let name = |kind: &Type| {
        match kind {
            Type::Object => "object",
            Type::String => "string",
            Type::Integer => "integer",
            Type::Number => "number",
            Type::Boolean => "boolean",
            Type::Array => "array",
            Type::Null => "null",
        }
        .to_owned()
    };
    match schema_type {
        SchemaType::Type(kind) => name(kind),
        SchemaType::Array(kinds) => {
            let spelled: Vec<String> = kinds
                .iter()
                .filter(|kind| !matches!(kind, Type::Null))
                .map(name)
                .collect();
            spelled.join(" or ")
        }
        SchemaType::AnyValue => "any".to_owned(),
    }
}
