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
    AzureOptions, GcsOptions, HEADERS, Headers, HfOptions, HttpOptions, S3Options, StorageOptions,
    WebdavOptions, WebdavTransport, is_flag, option_schemes,
};
pub use store::{
    Entry, RemoteDir, RemoteFile, SourceUrl, open, open_dir, open_mounted, open_mounted_dir,
    parse_url, supported_schemes,
};
