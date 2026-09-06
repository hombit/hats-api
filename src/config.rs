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
/// The default is every remote endpoint on the public internet, and no local files.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AccessConfig {
    pub network: NetworkConfig,
    pub s3: EndpointConfig,
    pub gcs: EndpointConfig,
    pub azure: EndpointConfig,
    pub http: HttpConfig,
    pub local: LocalConfig,
}

/// Which addresses a request may reach, whatever backend it goes through: the
/// destination is a property of the address rather than of the protocol. See
/// [`crate::network`].
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct NetworkConfig {
    /// `127.0.0.0/8`, `::1`, and `localhost`.
    pub allow_loopback: bool,
    /// Everything else that is not the public internet: RFC1918, link-local — which is
    /// where the cloud metadata services are — unique-local, and the reserved ranges.
    pub allow_private: bool,
    /// Names that only resolve inside a network: single-label ones, and anything whose
    /// last label is not a top-level domain IANA has delegated.
    pub allow_local_names: bool,
    /// Networks to allow whatever the switches above say, e.g. `["10.1.2.0/24"]`.
    pub allow_cidrs: Vec<String>,
    /// Hosts to allow whatever the switches above say, and whatever they resolve to.
    pub allow_hosts: Vec<String>,
}

/// One remote backend's endpoint rules. Every backend has the same shape, under its own
/// section: `[access.s3]`, `[access.gcs]`, `[access.azure]`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EndpointConfig {
    /// Endpoints a request may point the service at. The provider's own name —
    /// `"aws"`, `"gcp"`, `"azure"` — is that provider's own service, which is what a
    /// url with no `endpoint` option means; anything else is a URL, e.g.
    /// `"https://minio.example.com"`.
    ///
    /// Three states, and the difference between the first two matters: absent is any
    /// endpoint, an empty list turns the backend off, and a list is exactly those
    /// endpoints.
    pub endpoints: Option<Vec<String>>,
}

/// `http://` and `https://` urls, which are one backend reached two ways. The endpoint
/// list has the same three states as every other backend's; what is extra here is that
/// the url carries the scheme, so whether cleartext is acceptable is a question about
/// the request rather than about an option beside it.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpConfig {
    /// Servers a request may read from, as urls: `["https://data.example.com"]`. There
    /// is no provider name here — an `http(s)://` url names its own server, so there is
    /// no "the provider's own service" for one to mean.
    ///
    /// Three states, as elsewhere: absent is any server, an empty list turns `http://`
    /// and `https://` off, and a list is exactly those.
    pub endpoints: Option<Vec<String>>,
    /// Whether an `http://` url may be read when no endpoint list names one. Off by
    /// default: over cleartext nothing says the bytes came from the host the url names,
    /// and a parquet file that something on the path rewrote is a wrong answer rather
    /// than a failed request. Distinct from `network.allow_loopback`, which is about
    /// which address may be reached rather than what may be spoken to it.
    pub allow_plain_http: bool,
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
        assert_eq!(config.access.gcs.endpoints, None);
        assert_eq!(config.access.azure.endpoints, None);
        assert!(config.access.local.paths.is_empty());
        assert!(!config.access.network.allow_loopback);
        assert!(!config.access.network.allow_private);
        assert!(!config.access.network.allow_local_names);
        assert!(config.access.network.allow_cidrs.is_empty());
        assert!(config.access.network.allow_hosts.is_empty());
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
    /// pinned: no key at all is every endpoint, an empty list is none. Every backend's
    /// section, since they share the type and could stop sharing the behaviour.
    #[test]
    fn an_absent_endpoint_list_is_not_an_empty_one() {
        for section in ["s3", "gcs", "azure"] {
            let config = parse(&format!("[access.{section}]")).unwrap();
            let empty = parse(&format!("[access.{section}]\nendpoints = []")).unwrap();
            let (present, absent) = match section {
                "s3" => (empty.access.s3.endpoints, config.access.s3.endpoints),
                "gcs" => (empty.access.gcs.endpoints, config.access.gcs.endpoints),
                _ => (empty.access.azure.endpoints, config.access.azure.endpoints),
            };
            assert_eq!(absent, None, "{section}");
            assert_eq!(present, Some(Vec::new()), "{section}");
        }
    }

    /// A backend's section is set on its own, without disturbing the others.
    #[test]
    fn each_backend_has_its_own_section() {
        let config = parse("[access.gcs]\nendpoints = [\"gcp\"]").unwrap();
        assert_eq!(
            config.access.gcs.endpoints.as_deref(),
            Some(["gcp".to_owned()].as_slice())
        );
        assert_eq!(config.access.s3.endpoints, None);
        assert_eq!(config.access.azure.endpoints, None);
    }

    #[test]
    fn a_misspelled_key_is_an_error_rather_than_a_silent_default() {
        for toml in [
            "[server]\nadress = \"127.0.0.1\"",
            "[access.local]\nallow_symlinks = true",
            "[access.s3]\nendpoint = \"aws\"",
            "[access.gcs]\nendpoint = \"gcp\"",
            "[access.azure]\nendpoitns = []",
            "[access.network]\nallow_privte = true",
            "[access.network]\nallow_cidr = []",
            "[acess.network]\nallow_loopback = true",
            // Not sections this file has: the schemes are `gs` and `az`, but the
            // sections are named for the services.
            "[access.gs]\nendpoints = []",
            "[access.az]\nendpoints = []",
            // The keys these replaced, so an old config fails loudly.
            "[access]\nallow = []",
            "[access]\nfollow_symlinks = true",
            // Loopback is one case of a destination rule, and lives with the rest.
            "[access]\nallow_loopback = true",
        ] {
            assert!(parse(toml).is_err(), "{toml} was accepted");
        }
    }

    /// The example file says every value shown is the default, and that it is complete.
    /// Both halves are checked: it parses under `deny_unknown_fields`, so it cannot name
    /// a key that no longer exists, and it produces the default config, so no value in
    /// it has drifted from what it claims to show.
    #[test]
    fn the_example_config_is_the_default_configuration() {
        let example = parse(include_str!("../hats-api.example.toml")).unwrap();
        let default = Config::default();
        assert_eq!(
            format!("{example:?}"),
            format!("{default:?}"),
            "hats-api.example.toml no longer shows the defaults"
        );
    }

    #[test]
    fn rejects_an_address_that_is_not_one() {
        assert!(parse("[server]\naddress = \"example.com\"").is_err());
        assert!(parse("[server]\nport = 99999").is_err());
    }
}
