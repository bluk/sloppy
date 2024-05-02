//! Waynode is a distributed hash table node.

#![warn(
    rust_2018_idioms,
    missing_docs,
    missing_debug_implementations,
    unused_lifetimes,
    unused_qualifications
)]

use clap::Parser;
use cloudburst::dht::node::{Id, LocalId};
use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Instant,
};
use tokio::{
    net::UdpSocket,
    signal,
    sync::{mpsc, oneshot},
};
use tracing::info;

mod dht;
mod http;

use dht::Node;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))]
    dht_bind: IpAddr,
    #[arg(long, default_value_t = 6881)]
    dht_port: u16,
    #[arg(long, default_value_t = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)))]
    http_bind: IpAddr,
    #[arg(long, default_value_t = 8080)]
    http_port: u16,
    #[arg(long, default_values_t = vec![
        String::from("router.magnets.im:6881"),
        String::from("router.bittorent.com:6881"),
        String::from("router.utorrent.com:6881"),
        String::from("dht.transmissionbt.com:6881"),
    ])]
    bootstrap: Vec<String>,
}

fn get_config(local_id: LocalId) -> dht::Config {
    let mut config = dht::Config::new(local_id);
    config.set_client_version(Some("ab12".into()));
    config.set_is_read_only_node(true);
    config
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }

    tracing::info!("signal received, starting graceful shutdown");
}

#[tokio::main]
async fn main() -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        if libsystemd::logging::connected_to_journal() {
            use tracing_subscriber::prelude::*;
            let journald = tracing_journald::layer().expect("journald should be available");
            tracing_subscriber::registry().with(journald).init();
        } else {
            tracing_subscriber::fmt::init();
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        tracing_subscriber::fmt::init();
    }

    let args = Args::parse();

    let dht_socket = SocketAddr::new(args.dht_bind, args.dht_port);

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    let socket = UdpSocket::bind(dht_socket).await?;
    let local_id = Id::rand(&mut rand::thread_rng()).unwrap();
    info!(dht_socket = %dht_socket, %local_id, "listening...");

    let config = get_config(LocalId::from(local_id));
    let node: Node<SocketAddrV4> =
        Node::new(config, std::iter::empty(), args.bootstrap, Instant::now());

    let (dht_cmd_tx, dht_cmd_rx) = mpsc::channel(32);
    let (dht_completion_tx, dht_completion_rx) = oneshot::channel();
    let dht_handle = tokio::spawn(dht::dht_task(socket, node, dht_cmd_rx, dht_completion_tx));

    let http_socket = SocketAddr::new(args.http_bind, args.http_port);
    info!(http_socket = %http_socket, "http listening...");

    let (http_completion_tx, http_completion_rx) = oneshot::channel();
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel();

    let http_handle = tokio::spawn(http::http_task(
        http_socket,
        dht_cmd_tx.clone(),
        http_shutdown_rx,
        http_completion_tx,
    ));

    tokio::select! {
        _ = dht_completion_rx => {
        }
        _ = http_completion_rx => {
        }
        () = &mut shutdown => {
        }
    }

    drop(dht_cmd_tx);
    http_shutdown_tx.send(()).unwrap();

    dht_handle
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
    http_handle
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
}
