use std::{env, path::PathBuf, process::ExitCode, sync::Arc};

use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::access::AccessPolicy;
use crate::config::{CONFIG_ENV_VAR, Config, LogConfig, LogFormat};

mod access;
mod app;
mod config;
mod error;
mod parquet_out;
mod query;
mod storage;

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

/// `RUST_LOG` wins over the config file, the way it does everywhere else.
fn init_tracing(config: &LogConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.filter));
    let registry = tracing_subscriber::registry().with(filter);
    match config.format {
        LogFormat::Text => registry.with(fmt::layer().with_ansi(config.ansi)).init(),
        LogFormat::Json => registry.with(fmt::layer().json()).init(),
    }
}

/// `--config <path>`, or nothing. Anything else is a usage error: this is the whole
/// command line, and silently ignoring an argument is how a config file gets missed.
fn config_path(args: &[String]) -> Result<Option<PathBuf>, String> {
    match args {
        [] => Ok(env::var_os(CONFIG_ENV_VAR).map(PathBuf::from)),
        [flag] if flag == "-h" || flag == "--help" => Err(String::new()),
        [flag, path] if flag == "-c" || flag == "--config" => Ok(Some(PathBuf::from(path))),
        [flag] if flag == "-c" || flag == "--config" => Err(format!("{flag} needs a path")),
        [other, ..] => Err(format!("unexpected argument {other:?}")),
    }
}

/// Everything that can fail before the first request, failing in one place.
fn startup() -> Result<(Config, AccessPolicy), String> {
    let args: Vec<String> = env::args().skip(1).collect();
    let config = match config_path(&args)? {
        Some(path) => config::load(&path).map_err(|error| error.to_string())?,
        None => Config::default(),
    };
    let policy = AccessPolicy::new(&config.access).map_err(|error| error.to_string())?;
    Ok((config, policy))
}

#[tokio::main]
async fn main() -> ExitCode {
    // The config decides how to log, so nothing before this point can be logged.
    let (config, policy) = match startup() {
        Ok(started) => started,
        Err(message) => {
            if !message.is_empty() {
                eprintln!("{message}\n");
            }
            eprint!("{USAGE}");
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
                eprintln!("invalid HATS_API_LISTEN_ADDR {value:?}: {error}");
                return ExitCode::FAILURE;
            }
        },
        Err(_) => config.server.listen_addr(),
    };

    let listener = match tokio::net::TcpListener::bind(listen_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("failed to bind {listen_addr}: {error}");
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(
        %listen_addr,
        allows = %policy.allowed_schemes().join(", "),
        "listening"
    );
    if let Err(error) = axum::serve(listener, app::router(Arc::new(policy)))
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        eprintln!("server error: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to listen for shutdown signal");
    }
}
