//! The TOML configuration file, and nothing else.
//!
//! Every field has a default, so the file is optional and every key in it is optional
//! too. Unknown keys are an error rather than a silent no-op: this file decides what
//! the service is allowed to read, and a typo in it must not quietly widen or narrow
//! that.

use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Where to look for the file when `--config` is not given.
pub const CONFIG_ENV_VAR: &str = "HATS_API_CONFIG";

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: ServerConfig,
    pub access: AccessConfig,
    pub log: LogConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Address to listen on. The default is every interface, which is what a container
    /// needs; `127.0.0.1` keeps it on the machine.
    pub address: IpAddr,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            port: 80,
        }
    }
}

impl ServerConfig {
    pub fn listen_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address, self.port)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    /// A tracing filter, in `RUST_LOG` syntax: `hats_api=debug`, `warn`, and so on.
    /// `RUST_LOG` itself overrides this when it is set.
    pub filter: String,
    pub format: LogFormat,
    /// Colour. Off when the output is not a terminal, whatever this says.
    pub ansi: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// One human-readable line per event.
    Text,
    /// One JSON object per event, for a log collector to parse.
    Json,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            filter: "hats_api=info,tower_http=info".to_owned(),
            format: LogFormat::Text,
            ansi: true,
        }
    }
}

/// What the service may read. See [`crate::access`] for what the entries mean.
///
/// The default is every s3 endpoint but the loopback interface, and no local files.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AccessConfig {
    /// Whether a request may reach the loopback interface. Consulted only where the
    /// configuration did not name a host itself.
    pub allow_loopback: bool,
    pub s3: S3Config,
    pub local: LocalConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct S3Config {
    /// Endpoints a request may point the service at. `"aws"` is AWS S3 itself, which
    /// is what a url with no `endpoint` option means; anything else is a URL, e.g.
    /// `"https://minio.example.com"`.
    ///
    /// Three states, and the difference between the first two matters: absent is any
    /// endpoint, an empty list is no s3 at all, and a list is exactly those endpoints.
    pub endpoints: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct LocalConfig {
    /// Directories a request may read files from. Empty, the default, is no local
    /// file access at all.
    pub paths: Vec<String>,
    /// Whether a local path may go through a symlink.
    pub follow_symlinks: bool,
}

#[derive(Debug)]
pub enum ConfigError {
    Read(PathBuf, io::Error),
    Parse(PathBuf, toml::de::Error),
    /// An `[access]` entry — an endpoint or a directory — that means nothing.
    Rule(String, String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(path, error) => write!(f, "cannot read {}: {error}", path.display()),
            Self::Parse(path, error) => write!(f, "invalid config {}: {error}", path.display()),
            Self::Rule(entry, reason) => {
                write!(f, "invalid [access] entry {entry:?}: {reason}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

pub fn load(path: &Path) -> Result<Config, ConfigError> {
    let text =
        std::fs::read_to_string(path).map_err(|error| ConfigError::Read(path.to_owned(), error))?;
    toml::from_str(&text).map_err(|error| ConfigError::Parse(path.to_owned(), error))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(toml: &str) -> Result<Config, toml::de::Error> {
        toml::from_str(toml)
    }

    #[test]
    fn an_empty_file_is_the_default_configuration() {
        let config = parse("").unwrap();
        assert_eq!(config.server.listen_addr().to_string(), "0.0.0.0:80");
        // Absent, not empty: any endpoint rather than none.
        assert_eq!(config.access.s3.endpoints, None);
        assert!(config.access.local.paths.is_empty());
        assert!(!config.access.allow_loopback);
        assert!(!config.access.local.follow_symlinks);
    }

    #[test]
    fn every_key_can_be_set_on_its_own() {
        let config = parse("[server]\naddress = \"127.0.0.1\"").unwrap();
        assert_eq!(config.server.listen_addr().to_string(), "127.0.0.1:80");
        let config = parse("[server]\nport = 8080").unwrap();
        assert_eq!(config.server.listen_addr().to_string(), "0.0.0.0:8080");
        let config = parse("[access.local]\nfollow_symlinks = true").unwrap();
        assert!(config.access.local.follow_symlinks);
        // Setting one access key must not disturb the others.
        assert_eq!(config.access.s3.endpoints, None);
    }

    #[test]
    fn logging_has_defaults_and_can_be_set() {
        let config = parse("").unwrap();
        assert_eq!(config.log.filter, "hats_api=info,tower_http=info");
        assert_eq!(config.log.format, LogFormat::Text);
        assert!(config.log.ansi);

        let config = parse("[log]\nformat = \"json\"\nfilter = \"warn\"\nansi = false").unwrap();
        assert_eq!(config.log.format, LogFormat::Json);
        assert_eq!(config.log.filter, "warn");
        assert!(!config.log.ansi);

        assert!(parse("[log]\nformat = \"xml\"").is_err());
    }

    /// The one distinction in this file that a reader could get wrong, so it is
    /// pinned: no key at all is every endpoint, an empty list is none.
    #[test]
    fn an_absent_endpoint_list_is_not_an_empty_one() {
        assert_eq!(parse("[access.s3]").unwrap().access.s3.endpoints, None);
        assert_eq!(
            parse("[access.s3]\nendpoints = []")
                .unwrap()
                .access
                .s3
                .endpoints,
            Some(Vec::new())
        );
    }

    #[test]
    fn a_misspelled_key_is_an_error_rather_than_a_silent_default() {
        for toml in [
            "[server]\nadress = \"127.0.0.1\"",
            "[access.local]\nallow_symlinks = true",
            "[access.s3]\nendpoint = \"aws\"",
            "[acess]\nallow_loopback = true",
            // The keys these replaced, so an old config fails loudly.
            "[access]\nallow = []",
            "[access]\nfollow_symlinks = true",
        ] {
            assert!(parse(toml).is_err(), "{toml} was accepted");
        }
    }

    #[test]
    fn rejects_an_address_that_is_not_one() {
        assert!(parse("[server]\naddress = \"example.com\"").is_err());
        assert!(parse("[server]\nport = 99999").is_err());
    }
}
