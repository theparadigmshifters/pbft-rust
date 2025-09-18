use std::{
    sync::{Arc},
    vec,
};
use tracing::{debug, info};
use crate::{api, config::AppConfig};

pub async fn start(
    config: AppConfig,
    mut rx_cancel: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), pbft_core::error::Error> {
    let (inner_tx_cancel, inner_rx_cancel) = tokio::sync::broadcast::channel(10);

    let pbft_module = pbft_core::Pbft::new(config.pbft_config.clone())?;
    let pbft_module = Arc::new(pbft_module);

    let mut api_server = api::ApiServer::new(config.clone(), pbft_module.clone()).await;
    info!("starting api server...");
    let api_handle = tokio::spawn(async move { api_server.run(inner_rx_cancel).await });

    info!("wait for other nodes...");
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    let pbft_module_m = pbft_module;
    let pbft_executor_rx_cancel = inner_tx_cancel.subscribe();
    info!("starting pbft module...");
    let pbft_handle = tokio::spawn(async move {
        pbft_module_m
            .start(pbft_executor_rx_cancel)
            .await
    });

    // This is not ideal in case api_handle or pbft_handle crashes
    rx_cancel.recv().await.unwrap();
    info!("received cancel signal");
    inner_tx_cancel.send(()).unwrap();

    for h in vec![api_handle, pbft_handle] {
        debug!("waiting for task to finish...");
        h.await.expect("failed to join task");
    }

    info!(
        node_id = config.pbft_config.node_config.self_id.0,
        "Node stopped..."
    );

    Ok(())
}
