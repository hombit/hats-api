//! Which addresses a request may reach.
//!
//! [`crate::access`] decides which *endpoint* a request may name. That is not the same
//! question as which address the machine ends up connecting to, and the gap between the
//! two is where a service that fetches urls on a caller's behalf gets used to read
//! things its callers cannot: the instance metadata service on `169.254.169.254`, an
//! admin interface on RFC1918 space, a Kubernetes service on a name that only resolves
//! inside the cluster. So the destination is a decision of its own, in `[access.network]`
//! and made here.
//!
//! ```toml
//! [access.network]
//! allow_loopback = false     # 127.0.0.0/8, ::1, localhost
//! allow_private = false      # RFC1918, link-local, unique-local, and the rest
//! allow_local_names = false  # single-label names, .internal, .cluster.local, …
//! allow_cidrs = ["10.1.2.0/24"]
//! allow_hosts = ["minio.internal"]
//! ```
//!
//! Names and addresses are two layers and both are needed. An address rule alone lets an
//! internal host on public IP space through; a name rule alone is defeated by writing the
//! address down instead. So a name is judged before it is resolved, and every address it
//! resolves to is judged after.
//!
//! **Both halves run, and the second one runs at the last possible moment.** Checking a
//! name and then handing it to a client that resolves it again is DNS rebinding: the
//! answer that passed the check is not the answer that gets connected to. The check
//! therefore lives in the HTTP client's own resolver, whose return value *is* the set of
//! addresses the connection will be attempted against — see [`NetworkPolicy::transport`].
//!
//! An address written literally never reaches a resolver, so it cannot rebind either; it
//! is checked when the endpoint is authorized and cannot change afterwards.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use ipnet::IpNet;
use opendal::HttpTransporter;
use url::Host;

use crate::config::{ConfigError, NetworkConfig};
use crate::error::ApiError;

/// Ranges that are not the public internet, beyond loopback. Denied together, under
/// `allow_private`, because the reason to refuse them is one reason: an address here is
/// reachable from where this service runs and not from where its callers are.
///
/// Wider than "RFC1918": link-local carries the cloud metadata services, carrier-grade
/// NAT space is a provider's internal network, and the reserved ranges are only ever
/// reached by a route somebody added on purpose.
const PRIVATE_V4: &[&str] = &[
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    // RFC6598, carrier-grade NAT.
    "100.64.0.0/10",
    // RFC3927 link-local, which is where EC2 and GCE serve IAM credentials.
    "169.254.0.0/16",
    // RFC6890 IETF protocol assignments.
    "192.0.0.0/24",
    "192.0.2.0/24",
    "198.51.100.0/24",
    "203.0.113.0/24",
    // RFC2544 benchmarking.
    "198.18.0.0/15",
    "224.0.0.0/4",
    // Reserved, and the broadcast address with it.
    "240.0.0.0/4",
];

const PRIVATE_V6: &[&str] = &[
    // Unique local.
    "fc00::/7",
    // Link-local.
    "fe80::/10",
    "ff00::/8",
    "2001:db8::/32",
    // RFC6052 NAT64, which embeds an IPv4 address that would otherwise be judged as
    // IPv6 and pass.
    "64:ff9b::/96",
    "64:ff9b:1::/48",
    // Discard-only.
    "100::/64",
];

/// Suffixes that name something inside a network rather than on the internet. Not an
/// exhaustive list — there cannot be one — which is why the address rules run as well.
///
/// `localhost` is not here: it is the loopback interface by definition, and
/// `allow_loopback` is the switch that already means it.
const LOCAL_SUFFIXES: &[&str] = &[
    ".local",
    ".internal",
    ".intranet",
    ".private",
    ".corp",
    ".home",
    ".lan",
    ".home.arpa",
    ".cluster.local",
    ".svc",
];

/// The rules themselves, without the client built from them. Shared with the resolver,
/// which runs on every connection and must not copy them.
#[derive(Debug)]
struct Rules {
    allow_loopback: bool,
    allow_private: bool,
    allow_local_names: bool,
    allow_cidrs: Vec<IpNet>,
    /// Names the operator wrote down, lowercased. A name here skips both halves of the
    /// check, including whatever it resolves to: an operator naming `minio.internal` has
    /// said the thing that address rules would otherwise be guessing at.
    allow_hosts: HashSet<String>,
    /// The same permission for an operator who wrote an address instead of a name.
    allow_addrs: HashSet<IpAddr>,
    private_v4: Vec<IpNet>,
    private_v6: Vec<IpNet>,
}

/// What `[access.network]` came to, and the HTTP client that enforces it.
#[derive(Debug)]
pub struct NetworkPolicy {
    rules: Arc<Rules>,
    transport: HttpTransporter,
}

impl NetworkPolicy {
    /// `named` is every host the operator named in an endpoint list. Naming an endpoint
    /// is permission to reach it: these rules govern what a *caller* may point the
    /// service at, not what the deployment was configured for.
    pub fn new(config: &NetworkConfig, named: &[Host<String>]) -> Result<Self, ConfigError> {
        let mut allow_hosts: HashSet<String> = config
            .allow_hosts
            .iter()
            .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
            .collect();
        let mut allow_addrs = HashSet::new();
        for host in named {
            match host {
                Host::Domain(name) => {
                    allow_hosts.insert(name.trim_end_matches('.').to_ascii_lowercase());
                }
                Host::Ipv4(ip) => {
                    allow_addrs.insert(IpAddr::V4(*ip));
                }
                Host::Ipv6(ip) => {
                    allow_addrs.insert(IpAddr::V6(*ip));
                }
            }
        }
        let allow_cidrs = config
            .allow_cidrs
            .iter()
            .map(|entry| {
                entry.parse::<IpNet>().map_err(|error| {
                    ConfigError::Rule(
                        entry.to_owned(),
                        format!("{error}; expected a network like 10.1.2.0/24"),
                    )
                })
            })
            .collect::<Result<_, _>>()?;

        let rules = Arc::new(Rules {
            allow_loopback: config.allow_loopback,
            allow_private: config.allow_private,
            allow_local_names: config.allow_local_names,
            allow_cidrs,
            allow_hosts,
            allow_addrs,
            private_v4: parse_nets(PRIVATE_V4),
            private_v6: parse_nets(PRIVATE_V6),
        });
        let transport = build_transport(Arc::clone(&rules))?;
        Ok(Self { rules, transport })
    }

    /// The name half, run before anything is resolved, so that a caller who named
    /// something they may not reach is told which rule said no rather than watching a
    /// connection fail.
    ///
    /// A literal address is settled here for good: there is no resolution step to
    /// disagree with it later.
    pub fn authorize_host(&self, host: &Host<&str>) -> Result<(), ApiError> {
        let refused = match host {
            Host::Domain(name) => self.rules.check_name(name),
            Host::Ipv4(ip) => self.rules.check_addr(IpAddr::V4(*ip)),
            Host::Ipv6(ip) => self.rules.check_addr(IpAddr::V6(*ip)),
        };
        refused.map_err(ApiError::forbidden)
    }

    /// The HTTP transport every store is built on. Its resolver is the address half of
    /// this policy, which is what makes the check happen on the addresses that are
    /// actually connected to.
    pub fn transport(&self) -> HttpTransporter {
        self.transport.clone()
    }
}

fn parse_nets(entries: &[&str]) -> Vec<IpNet> {
    entries
        .iter()
        .filter_map(|entry| entry.parse().ok())
        .collect()
}

impl Rules {
    /// `Err` carries the reason, phrased for the caller who wrote the name.
    fn check_name(&self, name: &str) -> Result<(), String> {
        let name = name.trim_end_matches('.').to_ascii_lowercase();
        if self.allow_hosts.contains(&name) {
            return Ok(());
        }
        // A name that is the loopback interface by definition. Judged as loopback rather
        // than as a local name, so that one switch means one thing.
        if name == "localhost" || name.ends_with(".localhost") {
            return match self.allow_loopback {
                true => Ok(()),
                false => Err(format!(
                    "{name} is the loopback interface, which this server does not reach; \
                     set access.network.allow_loopback to change that"
                )),
            };
        }
        if self.allow_local_names {
            return Ok(());
        }
        // A name with no dot in it resolves through the search domains of whatever
        // network this process is on, which is the definition of somewhere internal.
        if !name.contains('.') {
            return Err(format!(
                "{name} is a single-label name, which only means anything inside this \
                 server's own network; set access.network.allow_local_names or name it \
                 in access.network.allow_hosts to change that"
            ));
        }
        match LOCAL_SUFFIXES.iter().find(|suffix| name.ends_with(*suffix)) {
            None => Ok(()),
            Some(suffix) => Err(format!(
                "{name} ends in {suffix}, which names a host inside a network rather than \
                 on the internet; set access.network.allow_local_names or name it in \
                 access.network.allow_hosts to change that"
            )),
        }
    }

    fn check_addr(&self, addr: IpAddr) -> Result<(), String> {
        // `::ffff:169.254.169.254` is the metadata service written as IPv6, and would
        // otherwise be judged against the IPv6 ranges, which it is in none of.
        let addr = match addr {
            IpAddr::V6(ip) => unmap(ip),
            v4 => v4,
        };
        if self.allow_addrs.contains(&addr) {
            return Ok(());
        }
        // An operator-written network is a deliberate grant, so it is consulted before
        // the categories — that is the whole point of being able to write one.
        if self.allow_cidrs.iter().any(|net| net.contains(&addr)) {
            return Ok(());
        }
        if addr.is_loopback() || addr.is_unspecified() {
            return match self.allow_loopback {
                true => Ok(()),
                false => Err(format!(
                    "{addr} is on the loopback interface, which this server does not \
                     reach; set access.network.allow_loopback to change that"
                )),
            };
        }
        let private = match addr {
            IpAddr::V4(ip) => self
                .private_v4
                .iter()
                .any(|net| net.contains(&IpAddr::V4(ip))),
            IpAddr::V6(ip) => self
                .private_v6
                .iter()
                .any(|net| net.contains(&IpAddr::V6(ip))),
        };
        match private && !self.allow_private {
            false => Ok(()),
            true => Err(format!(
                "{addr} is not on the public internet, and this server reaches only \
                 addresses that are; set access.network.allow_private, or name the \
                 network in access.network.allow_cidrs, to change that"
            )),
        }
    }

    /// What a name resolved to, judged before a connection is attempted against any of
    /// it. Every address has to pass: an answer that mixes a public address with
    /// `127.0.0.1` is an answer designed to be retried until the second one is used.
    fn check_resolved(&self, name: &str, addrs: &[SocketAddr]) -> Result<(), String> {
        if self
            .allow_hosts
            .contains(&name.trim_end_matches('.').to_ascii_lowercase())
        {
            return Ok(());
        }
        // The name itself as well: this runs on every connection the process makes, so
        // it is also the backstop for a host that reached the client without passing
        // `authorize_host` — a redirect, or a backend that builds its own url.
        self.check_name(name)?;
        addrs
            .iter()
            .try_for_each(|addr| self.check_addr(addr.ip()))
            .map_err(|reason| {
                format!("{name} resolves to an address this server will not reach: {reason}")
            })
    }
}

/// `::ffff:a.b.c.d` is an IPv4 address wearing an IPv6 spelling, and every IPv4 rule has
/// to apply to it.
fn unmap(ip: Ipv6Addr) -> IpAddr {
    match ip.octets() {
        [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, a, b, c, d] => {
            IpAddr::V4(Ipv4Addr::new(a, b, c, d))
        }
        _ => IpAddr::V6(ip),
    }
}

/// A resolver that answers only with addresses the policy allows.
///
/// This is the last thing between a name and a socket: whatever it returns is what the
/// connection is attempted against, so there is no second resolution for an attacker's
/// DNS server to answer differently.
struct PolicyResolver {
    rules: Arc<Rules>,
}

impl reqwest::dns::Resolve for PolicyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let rules = Arc::clone(&self.rules);
        Box::pin(async move {
            let name = name.as_str().to_owned();
            // Port 0: the connector replaces it with the url's own.
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host((name.as_str(), 0))
                .await
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) })?
                .collect();
            rules.check_resolved(&name, &addrs)?;
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

fn build_transport(rules: Arc<Rules>) -> Result<HttpTransporter, ConfigError> {
    let client = reqwest::Client::builder()
        .dns_resolver(Arc::new(PolicyResolver { rules }))
        // A redirect is the origin choosing the next destination, which is the one
        // decision this service does not let anything but its own config make: the hop
        // would carry the caller's credentials to a host no endpoint rule named. A 3xx
        // therefore comes back as the response it is.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| ConfigError::Rule("access.network".to_owned(), error.to_string()))?;
    Ok(HttpTransporter::new(
        opendal_http_transport_reqwest::ReqwestTransport::new(client),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(config: &NetworkConfig) -> Arc<Rules> {
        Arc::clone(&NetworkPolicy::new(config, &[]).unwrap().rules)
    }

    fn default_rules() -> Arc<Rules> {
        rules(&NetworkConfig::default())
    }

    fn addr(raw: &str) -> IpAddr {
        raw.parse().unwrap()
    }

    fn socket(raw: &str) -> SocketAddr {
        SocketAddr::new(addr(raw), 443)
    }

    /// The addresses this exists for. Every one of them is reachable from a machine in a
    /// cloud or a cluster and from nowhere a caller sits.
    #[test]
    fn the_default_policy_refuses_everything_that_is_not_the_public_internet() {
        let rules = default_rules();
        for raw in [
            // The reason this rule exists at all.
            "169.254.169.254",
            "127.0.0.1",
            "127.13.14.15",
            "0.0.0.0",
            "10.0.0.1",
            "172.16.5.4",
            "192.168.1.1",
            "100.64.0.1",
            "192.0.0.1",
            "198.18.0.1",
            "224.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fd00::1",
            "fe80::1",
            "ff02::1",
            // The same addresses in the spellings that are meant to slip past a check
            // that only knows the obvious ones.
            "::ffff:169.254.169.254",
            "::ffff:127.0.0.1",
            "64:ff9b::a9fe:a9fe",
        ] {
            assert!(rules.check_addr(addr(raw)).is_err(), "{raw} was allowed");
        }

        for raw in ["1.1.1.1", "52.95.110.1", "2606:4700::1111"] {
            assert!(rules.check_addr(addr(raw)).is_ok(), "{raw} was refused");
        }
    }

    #[test]
    fn each_switch_opens_only_what_it_names() {
        let loopback = rules(&NetworkConfig {
            allow_loopback: true,
            ..Default::default()
        });
        assert!(loopback.check_addr(addr("127.0.0.1")).is_ok());
        assert!(loopback.check_addr(addr("10.0.0.1")).is_err());

        let private = rules(&NetworkConfig {
            allow_private: true,
            ..Default::default()
        });
        assert!(private.check_addr(addr("10.0.0.1")).is_ok());
        assert!(private.check_addr(addr("169.254.169.254")).is_ok());
        // Loopback is its own switch, and the wider one does not imply it.
        assert!(private.check_addr(addr("127.0.0.1")).is_err());
    }

    /// A network the operator wrote down is a deliberate grant, and it does not open the
    /// category it is a member of.
    #[test]
    fn an_allowed_cidr_opens_that_network_and_no_other() {
        let rules = rules(&NetworkConfig {
            allow_cidrs: vec!["10.1.2.0/24".to_owned()],
            ..Default::default()
        });
        assert!(rules.check_addr(addr("10.1.2.3")).is_ok());
        assert!(rules.check_addr(addr("10.1.3.3")).is_err());
        assert!(rules.check_addr(addr("169.254.169.254")).is_err());
    }

    #[test]
    fn a_cidr_that_is_not_one_is_a_startup_error() {
        for entry in ["10.1.2.0/33", "not-a-network", "10.1.2.3"] {
            let config = NetworkConfig {
                allow_cidrs: vec![entry.to_owned()],
                ..Default::default()
            };
            assert!(
                NetworkPolicy::new(&config, &[]).is_err(),
                "{entry} was accepted"
            );
        }
    }

    /// The names half. An internal host on public address space defeats every address
    /// rule, so the name is judged too.
    #[test]
    fn the_default_policy_refuses_names_that_only_mean_something_internally() {
        let rules = default_rules();
        for name in [
            "metadata",
            "minio",
            "printer.local",
            "metadata.google.internal",
            "db.cluster.local",
            "api.default.svc",
            "nas.home.arpa",
            "gateway.lan",
            // A trailing dot is the same name.
            "metadata.google.internal.",
            // And case is not a distinction a resolver makes either.
            "Metadata.Google.Internal",
        ] {
            assert!(rules.check_name(name).is_err(), "{name} was allowed");
        }

        for name in [
            "example.com",
            "s3.us-east-1.amazonaws.com",
            "localhost.example.com",
        ] {
            assert!(rules.check_name(name).is_ok(), "{name} was refused");
        }
    }

    /// `localhost` is the loopback interface under another spelling, and it answers to
    /// the switch that already means that rather than to the local-names one.
    #[test]
    fn localhost_is_loopback_rather_than_a_local_name() {
        let names_allowed = rules(&NetworkConfig {
            allow_local_names: true,
            ..Default::default()
        });
        let error = names_allowed.check_name("localhost").unwrap_err();
        assert!(error.contains("allow_loopback"), "{error}");
        assert!(names_allowed.check_name("sub.localhost").is_err());

        let loopback = rules(&NetworkConfig {
            allow_loopback: true,
            ..Default::default()
        });
        assert!(loopback.check_name("localhost").is_ok());
        // And it opens only that: the other internal names are a separate decision.
        assert!(loopback.check_name("metadata.google.internal").is_err());
    }

    /// A host the operator named is reachable whatever the categories say about it —
    /// which is the whole reason to be able to name one.
    #[test]
    fn a_named_host_needs_no_further_permission() {
        let rules = rules(&NetworkConfig {
            allow_hosts: vec!["minio.internal".to_owned()],
            ..Default::default()
        });
        assert!(rules.check_name("minio.internal").is_ok());
        // Including what it resolves to: naming it would mean nothing otherwise, since
        // an internal name is internal precisely because it resolves inside.
        assert!(
            rules
                .check_resolved("minio.internal", &[socket("10.0.0.7")])
                .is_ok()
        );
        // And only that host.
        assert!(rules.check_name("other.internal").is_err());
    }

    /// An endpoint the operator put in `[access.s3]` is the same deliberate grant, and
    /// the connect-time check has to know about it or the deployment cannot reach it.
    #[test]
    fn an_endpoint_the_operator_configured_is_allowed_at_connect_time() {
        let policy = NetworkPolicy::new(
            &NetworkConfig::default(),
            &[
                Host::Domain("minio.internal".to_owned()),
                Host::Ipv4("10.4.5.6".parse().unwrap()),
            ],
        )
        .unwrap();
        assert!(
            policy
                .rules
                .check_resolved("minio.internal", &[socket("10.0.0.7")])
                .is_ok()
        );
        assert!(
            policy
                .authorize_host(&Host::Ipv4("10.4.5.6".parse().unwrap()))
                .is_ok()
        );
        assert!(
            policy
                .authorize_host(&Host::Ipv4("10.4.5.7".parse().unwrap()))
                .is_err()
        );
    }

    /// The rebinding case: a public name whose answer is not a public address. This is
    /// the check that only means anything because it runs in the resolver.
    #[test]
    fn a_public_name_that_resolves_inside_is_refused() {
        let rules = default_rules();
        let error = rules
            .check_resolved("rebind.example.com", &[socket("169.254.169.254")])
            .unwrap_err();
        assert!(error.contains("rebind.example.com"), "{error}");
        assert!(error.contains("169.254.169.254"), "{error}");
    }

    /// Every address, not the first: an answer that mixes them is an answer built to be
    /// retried until the useful one comes up.
    #[test]
    fn one_bad_address_in_an_answer_refuses_the_whole_answer() {
        let rules = default_rules();
        assert!(
            rules
                .check_resolved(
                    "mixed.example.com",
                    &[socket("1.1.1.1"), socket("127.0.0.1")]
                )
                .is_err()
        );
        assert!(
            rules
                .check_resolved("good.example.com", &[socket("1.1.1.1"), socket("8.8.8.8")])
                .is_ok()
        );
    }

    /// A name that never went through `authorize_host` — a redirect, a backend building
    /// its own url — still meets the name rules, because the resolver applies them too.
    #[test]
    fn the_resolver_judges_the_name_as_well_as_the_addresses() {
        let rules = default_rules();
        assert!(
            rules
                .check_resolved("metadata.google.internal", &[socket("1.1.1.1")])
                .is_err()
        );
    }

    /// The rules above only mean anything if they are what the HTTP client resolves
    /// with, so this drives the resolver itself — system lookup included — rather than
    /// the check behind it. `localhost` is the one name every machine resolves, and it
    /// resolves to an address the default policy refuses.
    #[tokio::test]
    async fn the_resolver_refuses_a_name_whose_addresses_the_policy_refuses() {
        use reqwest::dns::Resolve;

        let refused = PolicyResolver {
            rules: default_rules(),
        };
        assert!(refused.resolve("localhost".parse().unwrap()).await.is_err());

        let allowed = PolicyResolver {
            rules: rules(&NetworkConfig {
                allow_loopback: true,
                ..Default::default()
            }),
        };
        let addrs: Vec<SocketAddr> = allowed
            .resolve("localhost".parse().unwrap())
            .await
            .unwrap()
            .collect();
        assert!(!addrs.is_empty(), "the resolver answered with nothing");
        assert!(
            addrs.iter().all(|addr| addr.ip().is_loopback()),
            "{addrs:?}"
        );
    }

    #[test]
    fn a_refusal_says_which_setting_would_change_it() {
        let rules = default_rules();
        for (host, setting) in [
            ("127.0.0.1", "allow_loopback"),
            ("10.0.0.1", "allow_private"),
        ] {
            let error = rules.check_addr(addr(host)).unwrap_err();
            assert!(error.contains(setting), "{host}: {error}");
        }
        for (name, setting) in [
            ("localhost", "allow_loopback"),
            ("metadata", "allow_local_names"),
            ("db.cluster.local", "allow_local_names"),
        ] {
            let error = rules.check_name(name).unwrap_err();
            assert!(error.contains(setting), "{name}: {error}");
        }
    }
}
