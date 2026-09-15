//! Turning a user-supplied URL into something DataFusion can read.
//!
//! Everything storage-specific lives here: opening the url is `store`'s, what each backend's
//! options are is `options`'s, how each of them becomes a store is `backends`'s, and reading a
//! server that will not serve byte ranges is [`materialize`]'s.

mod backends;
pub mod materialize;
mod options;
mod store;

pub use options::{
    AzureOptions, GcsOptions, Headers, HttpOptions, S3Options, StorageOptions, WebdavOptions,
    WebdavTransport, option_schemes,
};
pub use store::{
    Entry, RemoteDir, RemoteFile, SourceUrl, open, open_dir, open_mounted, open_mounted_dir,
    parse_url, supported_schemes,
};
