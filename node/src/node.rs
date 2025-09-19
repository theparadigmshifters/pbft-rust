use std::{vec};
use tokio::sync::mpsc;
use tracing::{debug, info, error};
use crate::{config::AppConfig};
use pbft_core::{config::NodeId, p2p_node::P2pLibp2p};

pub async fn start(
    config: AppConfig,
    mut rx_cancel: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), pbft_core::error::Error> {
    let (inner_tx_cancel, inner_rx_cancel) = tokio::sync::broadcast::channel(10);
    let (msg_tx, msg_rx) = mpsc::unbounded_channel::<(NodeId, Vec<u8>)>();

    let mut p2p = P2pLibp2p::default_from_committee(config.pbft_config.clone().committee_config);
    p2p.init(inner_rx_cancel, move |id, payload: Vec<u8>| {
        if let Err(e) = msg_tx.send((id, payload)) {
            error!("Failed to send message to PBFT module: {}", e);
        }
        Ok(())
    }).expect("failed to initialize p2p node");
    info!("Local peer ID: {}", p2p.local_peer_id());
    info!("Local ID: {}", p2p.local_id().to_string());

    tokio::select! {
        result = p2p.wait_for_min_peers() => {
            match result {
                Ok(()) => {
                    info!("✅ Minimum peer connections established, proceeding with PBFT startup");
                }
                Err(e) => {
                    error!("Failed to establish minimum peer connections: {}", e);
                    return Err(pbft_core::error::Error::Internal(
                        format!("Insufficient peer connections: {}", e)
                    ));
                }
            }
        }
        _ = rx_cancel.recv() => {
            info!("Received cancel signal while waiting for peer connections");
            return Ok(());
        }
    }

    let pbft_msg_rx_cancel = inner_tx_cancel.subscribe();
    let mut pbft_module = pbft_core::Pbft::new(config.pbft_config.clone(), p2p, msg_rx, pbft_msg_rx_cancel)?;

    let pbft_executor_rx_cancel = inner_tx_cancel.subscribe();
    info!("starting pbft module...");
    let pbft_handle = tokio::spawn(async move {
        pbft_module
            .start(pbft_executor_rx_cancel)
            .await
    });

    // This is not ideal in case api_handle or pbft_handle crashes
    rx_cancel.recv().await.unwrap();
    info!("received cancel signal");
    inner_tx_cancel.send(()).unwrap();

    for h in vec![pbft_handle] {
        debug!("waiting for task to finish...");
        h.await.expect("failed to join task");
    }

    info!(
        node_id = config.pbft_config.node_config.self_id.0,
        "Node stopped..."
    );

    Ok(())
}
