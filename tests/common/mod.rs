use std::net::Ipv6Addr;
use std::process::ExitStatus;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::{select, spawn};

async fn free_port() -> anyhow::Result<u16> {
    TcpListener::bind((Ipv6Addr::LOCALHOST, 0))
        .await
        .context("failed to start TCP listener")?
        .local_addr()
        .context("failed to query listener local address")
        .map(|v| v.port())
}

async fn spawn_server(
    cmd: &mut Command,
) -> anyhow::Result<(JoinHandle<anyhow::Result<ExitStatus>>, oneshot::Sender<()>)> {
    let mut child = cmd
        .kill_on_drop(true)
        .spawn()
        .context("failed to spawn child")?;
    let (stop_tx, stop_rx) = oneshot::channel();
    let child = spawn(async move {
        select!(
            res = stop_rx => {
                res.context("failed to wait for shutdown")?;
                child.kill().await.context("failed to kill child")?;
                child.wait().await
            }
            status = child.wait() => {
                status
            }
        )
        .context("failed to wait for child")
    });
    Ok((child, stop_tx))
}

pub async fn start_nats() -> anyhow::Result<(
    u16,
    async_nats::Client,
    JoinHandle<anyhow::Result<ExitStatus>>,
    oneshot::Sender<()>,
)> {
    let port = free_port().await?;
    let (server, stop_tx) =
        spawn_server(Command::new("nats-server").args(["-V", "-T=false", "-p", &port.to_string()]))
            .await
            .context("failed to start NATS.io server")?;

    let client = wrpc_cli::nats::connect(format!("nats://localhost:{port}"))
        .await
        .context("failed to connect to NATS.io server")?;
    Ok((port, client, server, stop_tx))
}
