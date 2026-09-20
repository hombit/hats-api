//! Turning a user-supplied URL into something DataFusion can read.
//!
//! Everything storage-specific lives here: opening the url is `store`'s, what each backend's
//! options are is `options`'s, how each of them becomes a store is `backends`'s, reading a
//! server that will not serve byte ranges is [`materialize`]'s, a Hugging Face repository is
//! `huggingface`'s, and following one origin's redirect to another is `redirect`'s.

mod backends;
mod huggingface;
pub mod materialize;
mod options;
mod redirect;
mod store;

pub use options::{
    Authorities, AzureOptions, GcsOptions, HEADERS, Headers, HfOptions, HttpOptions, Opened,
    S3Options, StorageOptions, WebdavOptions, WebdavTransport, option_names, option_schemes,
};
pub use store::{
    Entry, Level, MountedBy, NamedBy, Object, RemoteDir, RemoteFile, SourceUrl, file_url, open,
    open_configured_dir, open_dir, open_mounted, open_mounted_dir, parse_url, supported_schemes,
};
