#![forbid(unsafe_code)]

use kata_device_plugin::{plugin, vfio};

use std::collections::HashSet;
use std::path::Path;

use plugin::DeviceServer;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// The one flag.  An argument templates directly in the DaemonSet spec, so
/// no config file or ConfigMap returns (KISS).
fn parse_naming() -> anyhow::Result<vfio::Naming> {
    let mut naming = vfio::Naming::Alias;
    for arg in std::env::args().skip(1) {
        match arg.strip_prefix("--resource-naming=").map(str::trim) {
            Some(value) => naming = vfio::Naming::parse(value).map_err(anyhow::Error::msg)?,
            None => anyhow::bail!("usage: kata-device-plugin [--resource-naming=alias|sku]"),
        }
    }
    Ok(naming)
}

/// Spawn one DeviceServer for `name`, tracked in `running`/`tasks`.
fn spawn_server(
    name: &str,
    naming: vfio::Naming,
    shutdown: &CancellationToken,
    running: &mut HashSet<String>,
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
) {
    info!(resource = %name, "starting plugin");
    let server = DeviceServer::new(
        name,
        naming,
        vfio::VFIO_DIR,
        vfio::SYSFS_DIR,
        plugin::SOCKET_DIR,
        plugin::CDI_DIR,
    );
    let token = shutdown.clone();
    let label = name.to_owned();
    tasks.push(tokio::spawn(async move {
        if let Err(e) = server.run(token).await {
            // {:#} keeps the error's cause chain; bare Display drops it.
            tracing::warn!(resource = %label, "plugin error: {e:#}");
        }
    }));
    running.insert(name.to_owned());
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kata_device_plugin=info".parse().unwrap()),
        )
        .init();
    let naming = parse_naming()?;
    info!(
        version = env!("CARGO_PKG_VERSION"),
        commit = env!("GIT_SHA"),
        naming = ?naming,
        "kata-device-plugin"
    );

    let shutdown = CancellationToken::new();
    let sd = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown");
        sd.cancel();
    });

    let mut running: HashSet<String> = HashSet::new();
    let mut tasks = Vec::new();

    // Alias names are known up front: start their servers unconditionally so
    // the kubelet sees the resource (with zero capacity if nothing is bound
    // yet) regardless of deployment ordering.  SKU names only exist once a
    // device is discovered, so sku mode relies on the rescan loop below.
    if matches!(naming, vfio::Naming::Alias) {
        for res in vfio::RESOURCES {
            spawn_server(res.name, naming, &shutdown, &mut running, &mut tasks);
        }
    }

    // Rescan loop: a resolved name that appears later (VFIO binding racing
    // the DaemonSet rollout; in sku mode, the first device of a new SKU)
    // gets its server spawned within one tick.  Servers are never stopped —
    // a name whose devices vanish keeps advertising zero capacity via its
    // own ListAndWatch poller.
    loop {
        let discovered = vfio::discover(
            Path::new(vfio::VFIO_DIR),
            Path::new(vfio::SYSFS_DIR),
            naming,
        );
        for name in discovered.keys() {
            if !running.contains(name) {
                spawn_server(name, naming, &shutdown, &mut running, &mut tasks);
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(plugin::POLL_INTERVAL) => {}
        }
    }

    futures::future::join_all(tasks).await;
    Ok(())
}
