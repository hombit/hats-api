use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

mod app;
mod error;
mod parquet_out;
mod query;
mod storage;

const DEFAULT_LISTEN_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("hats_api=info,tower_http=info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .init();
}

#[tokio::main]
async fn main() {
    init_tracing();

    let listen_addr = env::var("HATS_API_LISTEN_ADDR")
        .map(|value| {
            value.parse().unwrap_or_else(|error| {
                eprintln!("invalid HATS_API_LISTEN_ADDR {value:?}: {error}");
                std::process::exit(1);
            })
        })
        .unwrap_or(DEFAULT_LISTEN_ADDR);

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .unwrap_or_else(|error| {
            eprintln!("failed to bind {listen_addr}: {error}");
            std::process::exit(1);
        });

    tracing::info!(%listen_addr, "listening");
    axum::serve(listener, app::router())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap_or_else(|error| {
            eprintln!("server error: {error}");
            std::process::exit(1);
        });
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to listen for shutdown signal");
    }
}
