//! A Hugging Face repository, read over the Hub's own HTTP API.
//!
//! A url names a repository rather than a server — `hf://datasets/owner/name[@revision]/path`,
//! the spelling `fsspec` and DuckDB use, with the repository type written out rather than
//! defaulted. The Hub is the server, and an organisation running a private one points a
//! request at it with the same `endpoint` option the bucket-addressed backends take.
//!
//! Two of the Hub's routes answer everything this service needs, and they are different
//! shapes, which is why this backend is more than an endpoint:
//!
//! - **`GET {hub}/{repo}/resolve/{revision}/{path}`** hands back a file, by redirecting to a
//!   presigned url on a CDN for anything stored in LFS — which is every parquet file in a
//!   dataset. Following that hop is [`super::redirect`]'s.
//! - **`GET {hub}/api/{type}/{repo}/tree/{revision}/{path}`** lists, as JSON, a page at a
//!   time. An object store's listing is what a catalog with `hats_npix_suffix = "/"` is read
//!   through, and there is no second way to learn the names inside such a partition.
//!
//! **The request is the only source of the token.** OpenDAL's own `services-hf` is not used
//! and this is one of the reasons: it depends on `hf-xet`, which reads `HF_TOKEN` and
//! `HF_ENDPOINT` itself, below OpenDAL, where nothing on this side can prevent it. Here the
//! Hub is addressed by a url this module builds and the token arrives in the request or not
//! at all, so a deployment that happens to have a Hugging Face login on it answers a caller's
//! anonymous request anonymously.

use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use http::{HeaderMap, HeaderValue};
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use secrecy::ExposeSecret;
use url::Url;

use crate::error::ApiError;
use crate::storage::options::HfOptions;
use crate::storage::store::file_url;

/// The Hub, for a request that names no `endpoint` of its own. Written out here rather than
/// read from `HF_ENDPOINT`: an environment variable is a source of configuration this service
/// does not take, and one that decides where a credentialed request goes.
const HUB: &str = "https://huggingface.co";

/// How many entries one page of a listing asks for. The Hub's own maximum; a catalog is
/// thousands of files and every page is a request.
const TREE_PAGE: usize = 1000;

/// Which kind of repository a url names.
///
/// Written in the url rather than defaulted, because the two are different url spaces on the
/// Hub — a dataset is under `/datasets/`, a model is at the root — and a default would make
/// `hf://owner/name` mean one of them silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RepoKind {
    Dataset,
    Model,
}

impl RepoKind {
    /// What the url's first segment spells it.
    fn parse(host: &str) -> Option<Self> {
        match host {
            "datasets" => Some(Self::Dataset),
            "models" => Some(Self::Model),
            _ => None,
        }
    }

    /// The segment the Hub's web routes put in front of a repository id. Empty for a model,
    /// which is addressed at the root — the asymmetry is the Hub's, not this module's.
    fn web_prefix(self) -> &'static str {
        match self {
            Self::Dataset => "datasets/",
            Self::Model => "",
        }
    }

    /// The segment the Hub's JSON API puts in front of one, which is not the same word.
    fn api_segment(self) -> &'static str {
        match self {
            Self::Dataset => "datasets",
            Self::Model => "models",
        }
    }

    const DESCRIBE_ALL: &'static str = "datasets or models";
}

/// A repository, as the front of a key names it.
///
/// **Read off the key rather than fixed per store**, which is not a detail. DataFusion keys its
/// object stores by a url's scheme and authority alone, and every dataset repository shares the
/// authority `datasets` — so a store built for one repository would be registered over by the
/// next one, and a query naming two Hugging Face catalogs would read both through whichever was
/// registered last. A store here is the Hub, and the repository is part of what a key says,
/// which is the same shape `s3://bucket` has within a bucket.
#[derive(Debug, Clone)]
pub(super) struct HfRepo {
    kind: RepoKind,
    owner: String,
    name: String,
    revision: String,
    /// The two segments exactly as they were written — `owner/name` or `owner/name@revision` —
    /// which is how every key under this repository is spelled.
    ///
    /// Kept as written rather than rebuilt, because it is what a listing's entries are named
    /// with, and a rebuilt one would differ from the caller's url by exactly the `@revision`
    /// they did or did not write.
    written: String,
}

impl HfRepo {
    /// The repository an `hf://` url names, and nothing about where it is read from.
    ///
    /// This is the refusal a caller sees, so it says how a url is written. The store reads the
    /// same thing back out of each key with [`Self::from_key`], which cannot say anything
    /// useful — by then the url has been through here.
    ///
    /// The revision rides on the repository name — `owner/name@dev` — which is the spelling
    /// the rest of the ecosystem uses. It is deliberately not an option: an option would be a
    /// second place for it to be written, and a url carrying both would have to be refused
    /// rather than resolved.
    pub(super) fn parse(url: &Url) -> Result<Self, ApiError> {
        let kind = Self::kind_of(url)?;
        let key = url.path().trim_start_matches('/');
        Self::split(kind, key).map(|(repo, _)| repo).ok_or_else(|| {
            ApiError::bad_request(format!(
                "url {} names no repository this service can read; an hf url is written \
                     hf://<type>/<owner>/<name>[@revision]/<path>, where <type> is {}, and \
                     a name may only hold letters, digits, \"-\", \"_\" and \".\"",
                file_url(url),
                RepoKind::DESCRIBE_ALL
            ))
        })
    }

    /// Which kind of repository a url's authority names, which is the half of the url that is
    /// the same for every key a store built from it will see.
    pub(super) fn kind_of(url: &Url) -> Result<RepoKind, ApiError> {
        url.host_str()
            .filter(|host| !host.is_empty())
            .and_then(RepoKind::parse)
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "url {} does not name a repository type; an hf url is written \
                     hf://<type>/<owner>/<name>[@revision]/<path>, where <type> is {}",
                    file_url(url),
                    RepoKind::DESCRIBE_ALL
                ))
            })
    }

    /// The repository at the front of a key, and whatever the key names inside it.
    fn from_key(kind: RepoKind, key: &str) -> object_store::Result<(Self, String)> {
        Self::split(kind, key).ok_or_else(|| object_store::Error::Generic {
            store: "hf",
            source: "a key that does not begin with a repository".into(),
        })
    }

    /// The one reading of a key, which both of the above are.
    fn split(kind: RepoKind, key: &str) -> Option<(Self, String)> {
        let mut segments = key.split('/').filter(|segment| !segment.is_empty());
        let owner = segments.next()?;
        let written_name = segments.next()?;
        let rest: Vec<&str> = segments.collect();

        // Split on the last `@`: neither a repository name nor a revision may contain one, so
        // either boundary would do — but the last is the reading that survives a name this
        // service has not seen.
        let (name, revision) = match written_name.rsplit_once('@') {
            Some((name, revision)) => (name, revision),
            None => (written_name, "main"),
        };
        // Everything here ends up in a url this service then connects to, so each part is
        // restricted to what cannot mean something else — the reason `require_label` restricts
        // a hostname. A `/` or a `?` in what was meant to be a name moves the request somewhere
        // the policy never judged. The Hub's own rules are narrower, which is as it should be:
        // this is about what cannot be let through, not about validating on the Hub's behalf.
        if ![owner, name, revision].into_iter().all(is_safe_segment) {
            return None;
        }

        Some((
            Self {
                kind,
                owner: owner.to_owned(),
                name: name.to_owned(),
                revision: revision.to_owned(),
                written: format!("{owner}/{written_name}"),
            },
            rest.join("/"),
        ))
    }

    /// Where a file inside this repository is read from, as a path under the Hub.
    fn resolve_path(&self, rest: &str) -> String {
        format!(
            "{}{}/{}/resolve/{}/{}",
            self.kind.web_prefix(),
            self.owner,
            self.name,
            self.revision,
            rest.trim_start_matches('/')
        )
    }

    /// Where a listing of this repository is asked for. A different route from the one above,
    /// under `/api/`, and with the repository type spelled the other way.
    fn tree_url(&self, hub: &Url, rest: &str) -> Url {
        let mut url = join(
            hub,
            &format!(
                "api/{}/{}/{}/tree/{}/{}",
                self.kind.api_segment(),
                self.owner,
                self.name,
                self.revision,
                rest.trim_start_matches('/')
            ),
        );
        url.query_pairs_mut()
            .append_pair("recursive", "true")
            .append_pair("expand", "false")
            .append_pair("limit", &TREE_PAGE.to_string());
        url
    }

    /// How a key inside this repository is spelled, which is how the caller's url spelled it.
    fn key_of(&self, rest: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/{}", self.written, rest.trim_start_matches('/')))
    }
}

/// A path under the Hub, joined rather than parsed: every part of it is either a constant here
/// or a segment [`is_safe_segment`] has already restricted, so there is nothing in it that
/// could name another server — and joining keeps it that way whatever is added later.
fn join(hub: &Url, path: &str) -> Url {
    let base = hub.path().trim_end_matches('/').to_owned();
    let mut url = hub.clone();
    url.set_path(&format!("{base}/{}", path.trim_start_matches('/')));
    url
}

/// What a repository's parts may hold. See the comment in [`HfRepo::split`].
fn is_safe_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.".contains(c))
}

/// The Hub this request reads from: the `endpoint` option where one was given, and
/// `https://huggingface.co` otherwise.
pub(super) fn hub_url(endpoint: Option<&Url>) -> Result<Url, ApiError> {
    match endpoint {
        Some(endpoint) => Ok(endpoint.clone()),
        None => Url::parse(HUB).map_err(|error| {
            ApiError::internal(format!("the Hugging Face url is invalid: {error}"))
        }),
    }
}

/// The caller's token as the header the Hub takes.
///
/// A header rather than anything on a builder, for the reason `webdav_headers` is: the listing
/// is this crate's own request rather than the store's, and a credential configured on one of
/// them only would make a private repository readable through one route and not the other.
pub(super) fn hf_headers(options: &HfOptions) -> Result<HeaderMap, ApiError> {
    let mut headers = HeaderMap::new();
    if let Some(token) = &options.token {
        let mut value = HeaderValue::from_str(&format!("Bearer {}", token.expose_secret()))
            .map_err(|_| {
                ApiError::bad_request(
                    "the token contains characters a header cannot carry; it is the \
                     access token the Hub issues",
                )
            })?;
        value.set_sensitive(true);
        headers.insert(http::header::AUTHORIZATION, value);
    }
    Ok(headers)
}

/// The Hub as an object store: reads through the inner store, listings through its JSON API.
///
/// The inner store is an ordinary HTTP one pointed at the Hub, so everything a read needs —
/// ranges, retries, the policy's transport, the redirect hop — is already on it and none of it
/// is written again here. What is left is the two things that store cannot do: turn a key into
/// the Hub's `resolve` route, and list.
///
/// One repository type, and every repository of it. See [`HfRepo`] for why it is not one
/// repository per store.
pub(super) struct HfStore {
    inner: Arc<dyn ObjectStore>,
    kind: RepoKind,
    hub: Url,
    client: reqwest::Client,
    headers: HeaderMap,
}

impl HfStore {
    pub(super) fn new(
        inner: Arc<dyn ObjectStore>,
        kind: RepoKind,
        hub: Url,
        client: reqwest::Client,
        headers: HeaderMap,
    ) -> Self {
        Self {
            inner,
            kind,
            hub,
            client,
            headers,
        }
    }

    /// Another handle on the same Hub, sharing the one store and the one client.
    ///
    /// Not a derived `Clone`, for [`crate::storage::RemoteFile`]'s reason: a derive makes it
    /// cheap to copy something holding a credential around without meaning to, and the
    /// headers here are the caller's token.
    fn handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            kind: self.kind,
            hub: self.hub.clone(),
            client: self.client.clone(),
            headers: self.headers.clone(),
        }
    }

    /// A key as the inner store spells it: the path under the Hub that hands the file over.
    fn within(&self, location: &ObjectPath) -> object_store::Result<ObjectPath> {
        let (repo, rest) = HfRepo::from_key(self.kind, location.as_ref())?;
        Ok(ObjectPath::from(repo.resolve_path(&rest)))
    }

    /// One page of the Hub's tree listing, and where the next one is.
    async fn page(&self, url: Url) -> object_store::Result<(Vec<TreeEntry>, Option<Url>)> {
        let response = self
            .client
            .get(url)
            .headers(self.headers.clone())
            .send()
            .await
            .map_err(|error| generic("listing this repository failed", error))?;
        let response = response
            .error_for_status()
            .map_err(|error| generic("the Hub refused to list this repository", error))?;
        // Read before the body: taking the bytes consumes the response.
        let next = next_page(response.headers());
        let body = response
            .bytes()
            .await
            .map_err(|error| generic("the Hub's listing could not be read", error))?;
        let entries: Vec<TreeEntry> = serde_json::from_slice(&body)
            .map_err(|error| generic("the Hub's listing is not the JSON it should be", error))?;
        Ok((entries, next))
    }
}

/// One entry of the Hub's tree listing. Only the three fields this service reads are declared;
/// the route answers with several more, and a listing is not something to be strict about on
/// the Hub's behalf.
#[derive(serde::Deserialize)]
struct TreeEntry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
    /// The object's own size. For a file stored in LFS this is the file's size and not the
    /// pointer's — the pointer's is a separate field, which is why this one can be trusted.
    #[serde(default)]
    size: u64,
}

/// Where the Hub says the rest of a listing is. A `Link` header, as one page of a thousand
/// entries and a cursor — so a catalog of ten thousand partitions is ten requests and not one
/// answer that quietly stopped at the first page.
fn next_page(headers: &HeaderMap) -> Option<Url> {
    let link = headers.get(http::header::LINK)?.to_str().ok()?;
    link.split(',').find_map(|entry| {
        let (target, rel) = entry.split_once(';')?;
        if !rel.contains("rel=\"next\"") {
            return None;
        }
        Url::parse(target.trim().trim_start_matches('<').trim_end_matches('>')).ok()
    })
}

fn generic(
    message: &'static str,
    error: impl std::error::Error + Send + Sync + 'static,
) -> object_store::Error {
    // The url is not put in the message: it is this module's own and says nothing a caller
    // did not write, and the error underneath may quote what it was given.
    tracing::debug!(%error, "{message}");
    object_store::Error::Generic {
        store: "hf",
        source: Box::new(error),
    }
}

/// `Url`'s own `Debug` prints its `password` field, and the headers are the caller's token.
impl std::fmt::Debug for HfStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HfStore")
            .field("inner", &self.inner)
            .field("kind", &self.kind)
            .field("hub", &self.hub.as_str())
            .field(
                "headers",
                &format_args!("<{} header(s)>", self.headers.len()),
            )
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for HfStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HfStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for HfStore {
    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut result = self
            .inner
            .get_opts(&self.within(location)?, options)
            .await?;
        // The object is the one the caller named, so what comes back says so: the inner store
        // answers under its own shorter key, and anything reading the result back — a plan
        // entry, a log line — would otherwise name a file in a repository it never mentions.
        result.meta.location = location.clone();
        Ok(result)
    }

    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner
            .put_opts(&self.within(location)?, payload, options)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner
            .put_multipart_opts(&self.within(location)?, options)
            .await
    }

    /// Nothing here deletes — the service only ever reads, and the inner store has no write
    /// capability to refuse with. The keys are mapped anyway: an unmapped one would name a
    /// different object than the caller wrote, and a path that is wrong only where nothing
    /// takes it is a path that is wrong the day something does.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        let store = self.handle();
        self.inner.delete_stream(
            locations
                .map(move |location| store.within(&location?))
                .boxed(),
        )
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.handle();
        // A listing is of one repository, and the prefix is what says which — so a prefix
        // naming none is the one thing this cannot answer. It becomes a stream of one error
        // rather than an empty one, which is the answer a caller cannot tell from a
        // repository holding nothing.
        let start = match prefix.map(|prefix| HfRepo::from_key(self.kind, prefix.as_ref())) {
            Some(Ok((repo, rest))) => (repo.tree_url(&self.hub, &rest), repo),
            Some(Err(error)) => return futures::stream::once(async move { Err(error) }).boxed(),
            None => {
                return futures::stream::once(async {
                    Err(object_store::Error::NotSupported {
                        source: "a listing has to name the repository to list".into(),
                    })
                })
                .boxed();
            }
        };
        let (start, repo) = start;

        // One request per page, each one saying where the next is, until the Hub stops
        // offering a `next`.
        futures::stream::try_unfold(Some(start), move |next| {
            let store = store.handle();
            let repo = repo.clone();
            async move {
                let Some(url) = next else { return Ok(None) };
                let (entries, following) = store.page(url).await?;
                let page: Vec<ObjectMeta> = entries
                    .into_iter()
                    // Directories are in the listing too, and an object store has no such
                    // thing: one turned into an entry is an object of no bytes that every
                    // read of it then fails on.
                    .filter(|entry| entry.kind == "file")
                    .map(|entry| ObjectMeta {
                        // Named the way the caller's url spelled the repository, since that
                        // is what every other key in this answer is relative to.
                        location: repo.key_of(&entry.path),
                        // The tree route carries no modification time. `None` is not
                        // available in an `ObjectMeta`, and the epoch is what
                        // `object_store`'s own backends use where an origin says nothing.
                        // Nothing here reads it; anything that later wants to know whether
                        // an object has changed should ask the Hub for an `ETag` rather
                        // than believe this field.
                        last_modified: chrono::DateTime::default(),
                        size: entry.size,
                        e_tag: None,
                        version: None,
                    })
                    .collect();
                Ok::<_, object_store::Error>(Some((page, following)))
            }
        })
        .map_ok(|page| futures::stream::iter(page.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        // The tree route lists recursively or not at all, and nothing here asks for the
        // delimited form. A wrong answer would be one that looks like an empty directory.
        Err(object_store::Error::NotSupported {
            source: "a Hugging Face repository is listed recursively".into(),
        })
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner
            .copy_opts(&self.within(from)?, &self.within(to)?, options)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::parse_url;

    fn repo(raw: &str) -> HfRepo {
        HfRepo::parse(&parse_url(raw).unwrap()).unwrap()
    }

    fn hub() -> Url {
        Url::parse(HUB).unwrap()
    }

    /// A key becomes the path that hands the file over, and that path is a different shape per
    /// repository type: the web route puts `datasets/` in front of a dataset and nothing in
    /// front of a model, while the API route spells the type the other way under `/api/`.
    #[test]
    fn a_key_addresses_both_of_the_hubs_routes() {
        let (dataset, rest) =
            HfRepo::from_key(RepoKind::Dataset, "UniverseTBD/mmu_gz10/dataset/x.parquet").unwrap();
        assert_eq!(rest, "dataset/x.parquet");
        assert_eq!(
            dataset.resolve_path(&rest),
            "datasets/UniverseTBD/mmu_gz10/resolve/main/dataset/x.parquet"
        );
        assert!(dataset.tree_url(&hub(), "dataset").as_str().starts_with(
            "https://huggingface.co/api/datasets/UniverseTBD/mmu_gz10/tree/main/dataset?"
        ));

        let (model, rest) = HfRepo::from_key(RepoKind::Model, "owner/name/config.json").unwrap();
        assert_eq!(
            model.resolve_path(&rest),
            "owner/name/resolve/main/config.json"
        );
    }

    /// **A repository is not a catalog.** A repository holds a directory tree, and a HATS
    /// catalog is wherever in it somebody put one — at the root, or several levels down, or
    /// several catalogs side by side in one repository. The first two segments are the
    /// repository because that is what the Hub's routes take; everything after them is a path
    /// inside it and is passed through untouched.
    #[test]
    fn a_catalog_may_be_anywhere_inside_a_repository() {
        // At the root, which is where a collection usually sits.
        let (repo, rest) = HfRepo::from_key(RepoKind::Dataset, "org/repo/hats.properties").unwrap();
        assert_eq!(rest, "hats.properties");
        assert_eq!(
            repo.resolve_path(&rest),
            "datasets/org/repo/resolve/main/hats.properties"
        );

        // And several levels down, which is the case a repository holding more than one
        // catalog has to have.
        let deep = "org/repo/hats/gaia/dataset/Norder=0/Dir=0/Npix=1.parquet";
        let (repo, rest) = HfRepo::from_key(RepoKind::Dataset, deep).unwrap();
        assert_eq!(rest, "hats/gaia/dataset/Norder=0/Dir=0/Npix=1.parquet");
        assert_eq!(
            repo.resolve_path(&rest),
            "datasets/org/repo/resolve/main/hats/gaia/dataset/Norder=0/Dir=0/Npix=1.parquet"
        );
        // The listing route takes the same path, so a catalog down there is discoverable as
        // well as readable.
        assert!(
            repo.tree_url(&hub(), &rest)
                .as_str()
                .starts_with("https://huggingface.co/api/datasets/org/repo/tree/main/hats/gaia/")
        );
    }

    /// A revision rides on the repository name, and a key keeps whichever spelling the caller
    /// wrote — a listing's entries are named with it, and every other key in the answer came
    /// from the caller's own url.
    #[test]
    fn a_revision_is_part_of_the_name_and_of_the_key() {
        let pinned = repo("hf://datasets/owner/name@6f4e2b1/dataset/x.parquet");
        assert_eq!(pinned.revision, "6f4e2b1");
        assert_eq!(pinned.name, "name");
        assert_eq!(pinned.written, "owner/name@6f4e2b1");
        assert_eq!(
            pinned.resolve_path("dataset/x.parquet"),
            "datasets/owner/name/resolve/6f4e2b1/dataset/x.parquet"
        );
        assert_eq!(
            pinned.key_of("dataset/x.parquet").as_ref(),
            "owner/name@6f4e2b1/dataset/x.parquet"
        );

        let floating = repo("hf://datasets/owner/name/dataset/x.parquet");
        assert_eq!(floating.revision, "main");
        assert_eq!(floating.written, "owner/name");
    }

    /// One store answers for every repository of its type, because that is how DataFusion
    /// keys one — so a key is what says which repository, and two of them go to two places
    /// through the same store rather than the second replacing the first.
    #[test]
    fn two_repositories_are_two_paths_through_one_store() {
        let (first, rest) = HfRepo::from_key(RepoKind::Dataset, "a/one/x.parquet").unwrap();
        let (second, other) = HfRepo::from_key(RepoKind::Dataset, "b/two@dev/y.parquet").unwrap();
        assert_eq!(
            first.resolve_path(&rest),
            "datasets/a/one/resolve/main/x.parquet"
        );
        assert_eq!(
            second.resolve_path(&other),
            "datasets/b/two/resolve/dev/y.parquet"
        );
    }

    /// A repository name is not a prefix of another's key. `owner/name` and `owner/name2` are
    /// two repositories, and a reading that took the first for a prefix of the second would
    /// read one repository's file under the other's name.
    #[test]
    fn a_repository_is_two_whole_segments() {
        let (repo, rest) = HfRepo::from_key(RepoKind::Dataset, "owner/name2/x.parquet").unwrap();
        assert_eq!(repo.name, "name2");
        assert_eq!(rest, "x.parquet");
    }

    /// Anything out of the url that ends up in a url this service connects to is restricted
    /// to what cannot mean something else. A `/` in a repository name moves the rest of the
    /// path, and the segments after `resolve` are how a file is named.
    ///
    /// The encoded spellings are the ones that matter: `Url` leaves `%2F` and `%20` in the
    /// path as written, so a check that only looked for a literal `/` would pass each of
    /// these and hand the escape on to whatever decodes it next.
    #[test]
    fn a_segment_that_could_redirect_the_request_is_refused() {
        for raw in [
            "hf://datasets/owner/name@..%2F..%2Fother/x.parquet",
            "hf://datasets/ow ner/name/x.parquet",
            "hf://datasets/owner/na%2Fme/x.parquet",
            "hf://datasets/..%2F..%2Fapi/name/x.parquet",
        ] {
            let url = parse_url(raw).unwrap();
            let error = HfRepo::parse(&url).unwrap_err();
            assert!(matches!(error, ApiError::BadRequest(_)), "{raw}: {error}");
        }
    }

    /// A query string is refused a step earlier, by the rule every scheme here shares — so
    /// it is not this check's to make, and a reader should not conclude from the case above
    /// that `?` reaches the repository parse at all.
    #[test]
    fn a_query_string_never_reaches_the_repository() {
        let url = parse_url("hf://datasets/owner/name/x.parquet?token=leaked").unwrap();
        assert_eq!(url.path(), "/owner/name/x.parquet");
    }

    /// The type is written out, and a url that names none is refused rather than read as one
    /// of them — `hf://owner/name` would be a model to the Hub and a dataset to whoever wrote
    /// it about astronomy data.
    #[test]
    fn a_url_without_a_repository_type_says_what_the_types_are() {
        for raw in ["hf://owner/name/x.parquet", "hf://spaces/owner/name/x.py"] {
            let url = parse_url(raw).unwrap();
            let error = HfRepo::parse(&url).unwrap_err();
            assert!(error.to_string().contains("datasets or models"), "{error}");
        }

        let no_repo = parse_url("hf://datasets/owner").unwrap();
        let error = HfRepo::parse(&no_repo).unwrap_err();
        assert!(error.to_string().contains("names no repository"), "{error}");
    }

    /// The listing is paginated, and a catalog is larger than one page — so a reader that
    /// stopped at the first would answer with a catalog missing most of its partitions,
    /// which reads as a small catalog rather than as a failure.
    #[test]
    fn the_next_page_is_read_off_the_link_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::LINK,
            HeaderValue::from_static(
                "<https://huggingface.co/api/datasets/o/n/tree/main?cursor=abc>; rel=\"next\"",
            ),
        );
        assert_eq!(
            next_page(&headers).unwrap().as_str(),
            "https://huggingface.co/api/datasets/o/n/tree/main?cursor=abc"
        );

        // The last page carries no such link, which is how the walk stops.
        assert!(next_page(&HeaderMap::new()).is_none());
    }
}
