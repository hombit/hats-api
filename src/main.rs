use std::{env, path::PathBuf, process::ExitCode, sync::Arc};

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

use hats_api::access::AccessPolicy;
use hats_api::app;
use hats_api::config::{self, CONFIG_ENV_VAR, Config, LogConfig, LogFormat};
use hats_api::logging;
use hats_api::mount::Mounts;

const USAGE: &str = "\
usage: hats-api [--config <path>]

  -c, --config <path>  TOML configuration file; defaults to $HATS_API_CONFIG,
                       and to the built-in defaults when neither is given
  -h, --help           this message

environment:
  HATS_API_CONFIG        configuration file to read
  HATS_API_LISTEN_ADDR   address:port to listen on, overriding the config file
  RUST_LOG               tracing filter, overriding the config file
";

/// `RUST_LOG` wins over the config file, the way it does everywhere else — except over
/// the targets [`hats_api::logging`] silences, which nothing may re-enable.
fn init_tracing(config: &LogConfig) {
    let registry = tracing_subscriber::registry().with(logging::filter(&config.filter));
    match config.format {
        LogFormat::Text => registry.with(fmt::layer().with_ansi(config.ansi)).init(),
        LogFormat::Json => registry.with(fmt::layer().json()).init(),
    }
}

/// Why the service is not going to serve. Two outcomes rather than one, because asking
/// for the usage is not a failure: it goes to stdout and exits zero, the way every other
/// command does, so `hats-api --help | less` reads it and a script does not think the
/// binary is broken.
enum NotServing {
    HelpRequested,
    Invalid(String),
}

/// `--config <path>`, or nothing. Anything else is a usage error: this is the whole
/// command line, and silently ignoring an argument is how a config file gets missed.
fn config_path(args: &[String]) -> Result<Option<PathBuf>, NotServing> {
    match args {
        [] => Ok(env::var_os(CONFIG_ENV_VAR).map(PathBuf::from)),
        [flag] if flag == "-h" || flag == "--help" => Err(NotServing::HelpRequested),
        [flag, path] if flag == "-c" || flag == "--config" => Ok(Some(PathBuf::from(path))),
        [flag] if flag == "-c" || flag == "--config" => {
            Err(NotServing::Invalid(format!("{flag} needs a path")))
        }
        [other, ..] => Err(NotServing::Invalid(format!(
            "unexpected argument {other:?}"
        ))),
    }
}

/// The one thing this binary writes to stdout. `print_stdout` is denied across the crate
/// so that nothing in a request path can write there — a service's stdout is its log —
/// and answering `--help` is the single case that belongs there instead.
#[expect(
    clippy::print_stdout,
    reason = "requested output, written before there is any logging to write it to"
)]
fn print_usage() {
    print!("{USAGE}");
}

/// Everything that can fail before the first request, failing in one place.
fn startup() -> Result<(Config, app::Service), NotServing> {
    let args: Vec<String> = env::args().skip(1).collect();
    let invalid = |error: &dyn std::fmt::Display| NotServing::Invalid(error.to_string());
    let config = match config_path(&args)? {
        Some(path) => config::load(&path).map_err(|error| invalid(&error))?,
        None => Config::default(),
    };
    // Before the policy, which reads them: a mount is the local half of what the service
    // may read, and the two must be looking at one list rather than two copies of it.
    let mounts = Arc::new(Mounts::new(&config.mounts, &config.data).map_err(|e| invalid(&e))?);
    let policy = AccessPolicy::new(&config.api.access, Arc::clone(&mounts))
        .map_err(|error| invalid(&error))?;
    let service = app::Service::new(
        policy,
        &config.limits,
        mounts,
        &config.api,
        &config.data,
        &config.server,
    )
    .map_err(|error| invalid(&error))?;
    Ok((config, service))
}

/// The mounts as one line of the startup log: what is readable, out of where, and which
/// of them the file server publishes rather than only answering questions about.
fn describe_mounts(mounts: &Mounts) -> String {
    match mounts.is_empty() {
        true => "none".to_owned(),
        false => mounts
            .iter()
            .map(|mount| {
                let served = match mount.serve() {
                    true => "",
                    false => " (api only)",
                };
                format!("{} -> {}{served}", mount.prefix(), mount.source().display())
            })
            .collect::<Vec<_>>()
            .join(", "),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    // The config decides how to log, so nothing before this point can be logged: these
    // two go to the terminal, and everything after `init_tracing` goes through it.
    let (config, service) = match startup() {
        Ok(started) => started,
        Err(NotServing::HelpRequested) => {
            print_usage();
            return ExitCode::SUCCESS;
        }
        Err(NotServing::Invalid(message)) => {
            eprintln!("{message}\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    init_tracing(&config.log);

    // The env var stays as an override because a container is often the one place the
    // config file is baked in and the port is not.
    let listen_addr = match env::var("HATS_API_LISTEN_ADDR") {
        Ok(value) => match value.parse() {
            Ok(addr) => addr,
            Err(error) => {
                tracing::error!(%error, value, "invalid HATS_API_LISTEN_ADDR");
                return ExitCode::FAILURE;
            }
        },
        Err(_) => config.server.listen_addr(),
    };

    let listener = match tokio::net::TcpListener::bind(listen_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!(%error, %listen_addr, "failed to bind");
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(
        %listen_addr,
        allows = %service.policy.allowed_schemes().join(", "),
        mounts = %describe_mounts(&service.mounts),
        "listening"
    );
    if let Err(error) = axum::serve(listener, app::router(service))
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        tracing::error!(%error, "server error");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to listen for shutdown signal");
    }
}
