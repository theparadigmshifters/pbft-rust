use std::{
    collections::{HashMap},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    async_trait,
    body::Body,
    extract::{FromRequest},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use futures::{stream::FuturesUnordered};
use pbft_core::{
    api::ClientRequestBroadcast, api::ProtocolMessageBroadcast, ClientRequest, ClientRequestId, OperationAck,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::{debug, error, info};

use crate::{
    config::AppConfig
};

pub const REQUEST_ID_HEADER: &str = "request-id";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Request error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Failed to broadcast request to any replica")]
    FailedToBroadcast,
}

trait VerifySignature {
    fn verify_signature(
        &self,
        replica_id: u64,
        signature: &str,
        msg: &[u8],
    ) -> Result<(), axum::response::Response>;
}

pub struct ApiServer {
    addr: SocketAddr,
    ctx: HandlerContext,
}

pub struct APIContext {
    pbft_module: Arc<pbft_core::Pbft>,
    client: reqwest::Client,
    pbft_leader_url: Mutex<String>,
    nodes_urls: HashMap<u64, String>,
}

type HandlerContext = Arc<APIContext>;

impl VerifySignature for HandlerContext {
    fn verify_signature(
        &self,
        peer_id: u64,
        signature: &str,
        msg: &[u8],
    ) -> Result<(), axum::response::Response> {
        self.pbft_module
            .verify_request_signature(peer_id, signature, msg)
            .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()).into_response())
    }
}

impl ApiServer {
    pub async fn new(
        config: AppConfig,
        pbft_module: Arc<pbft_core::Pbft>,
    ) -> Self {
        let addr = SocketAddr::from((config.listen_addr, config.port));

        let api_context = Arc::new(APIContext {
            pbft_module,
            client: reqwest::Client::new(),
            pbft_leader_url: Mutex::new(config.node_url()),
            nodes_urls: config
                .pbft_config
                .node_config
                .nodes
                .iter()
                .map(|node| (node.id.0, node.addr.clone()))
                .collect(),
        });

        Self {
            addr,
            ctx: api_context,
        }
    }

    pub async fn run(&mut self, mut rx_shutdown: tokio::sync::broadcast::Receiver<()>) {
        let kv_router = Router::new()
            .route("/", post(handle_block));

        // TODO: Paths could probably be better...
        // KV Node translates the request to pBFT operation and sends it here
        let consensus_ext_router = Router::new()
            // Request operation to execute
            .route("/operation", post(handle_consensus_operation_execute));

        // pBFT nodes talking to each other
        let consensus_int_router = Router::new()
            // Client Request + PrePrepare broadcasted by the leader
            .route("/execute", post(handle_consensus_pre_prepare))
            // Any other consensus message -- Prepare, Commit, ViewChange, NewView
            .route("/message", post(handle_consensus_message))
            // Debuging endpoint to dump current pBFT state
            .route("/state", get(handle_state_dump));

        // Combine routers
        let app = Router::new()
            .route("/api/v1/health", get(health_handler))
            .nest("/api/v1/kv", kv_router)
            .nest("/api/v1/consensus", consensus_ext_router)
            .nest("/api/v1/pbft", consensus_int_router)
            .with_state(self.ctx.clone());

        let server = axum::Server::bind(&self.addr);

        // Disable keep alive for tests so that we do not wait long on graceful
        // shutdown.
        #[cfg(test)]
        let server = server
            .http1_keepalive(false)
            .tcp_keepalive(Some(Duration::from_secs(1)));

        let server = server
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move {
                rx_shutdown
                    .recv()
                    .await
                    .expect("failed to await for cancel receive");
                info!("gracefully shutting down server...");
            });

        info!(addr = ?self.addr, "server starting to listen");

        server.await.expect("failed to await for server");

        info!(addr = ?self.addr, "Server shut down!",);
    }
}

pub struct JsonAuthenticated<T: DeserializeOwned> {
    pub sender_id: u64,
    pub data: T,
}

pub struct JsonAuthenticatedExt<T: DeserializeOwned>(pub JsonAuthenticated<T>);

#[async_trait]
impl<S, T: DeserializeOwned> FromRequest<S, Body> for JsonAuthenticatedExt<T>
where
    S: VerifySignature + Send + Sync,
{
    type Rejection = axum::response::Response;

    async fn from_request(
        req: axum::http::Request<Body>,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let (parts, body) = req.into_parts();

        let signature = get_replica_signature(&parts.headers)?;
        let peer_id = get_sender_replica_id(&parts.headers)?;

        // this wont work if the body is an long running stream -- it is fine
        let bytes = hyper::body::to_bytes(body)
            .await
            .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response())?;

        state.verify_signature(peer_id, &signature, &bytes)?;

        let data = serde_json::from_slice(&bytes)
            .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()).into_response())?;

        Ok(JsonAuthenticatedExt(JsonAuthenticated {
            sender_id: peer_id,
            data: data,
        }))
    }
}

async fn health_handler(_ctx: axum::extract::State<HandlerContext>) -> &'static str {
    "Ok"
}

// KV router handlers

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockRequest {
    pub block: String,
}

async fn handle_block(
    ctx: axum::extract::State<HandlerContext>,
    headers: HeaderMap,
    Json(request): Json<BlockRequest>,
) -> impl axum::response::IntoResponse {
    let operation = pbft_core::Operation {
        block: request.block.as_bytes().to_vec()
    };

    handle_operation(ctx, headers, operation).await
}

async fn handle_operation(
    ctx: axum::extract::State<HandlerContext>,
    headers: HeaderMap,
    operation: pbft_core::Operation,
) -> impl axum::response::IntoResponse {
    let request_id = get_request_id(&headers)?;
    let request_id = ClientRequestId(request_id);

    info!(
        request_id = request_id.to_string(),
        operation = ?operation,
        "handling kv operation",
    );

    // Send request to consensus layer
    let client_req = ClientRequest {
        request_id: request_id.clone(),
        operation: operation.clone(),
    };
    let _ = match send_consensus_request(&ctx, &client_req).await {
        Ok(ack) => ack,
        Err(err) => {
            error!(error = ?err, "failed to send request to consensus layer");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "failed to send request to consensus layer",
            )
                .into_response());
        }
    };
    
    Ok((StatusCode::OK, "OK"))
}

async fn send_consensus_request(
    ctx: &HandlerContext,
    msg: &ClientRequest,
) -> Result<OperationAck, Error> {
    let url = ctx.pbft_leader_url.lock().unwrap().clone();
    let resp = ctx
        .client
        .post(format!("{}/api/v1/consensus/operation", url))
        .json(msg)
        .timeout(Duration::from_secs(5))
        .send()
        .await;

    let resp = if let Err(err) = resp {
        error!(error = ?err, "failed to send request to last known leader, broadcasting to all");
        broadcast_request_to_all(ctx, msg).await?
    } else {
        let resp = resp.unwrap();
        if !resp.status().is_success() {
            error!(status = ?resp.status(), "failed to send request to last known leader, broadcasting to all");
            broadcast_request_to_all(ctx, msg).await?
        } else {
            resp
        }
    };

    let operation_ack = resp.json::<OperationAck>().await?;
    // TODO: We could update only when it changes using some RwLock
    if let Some(leader_url) = ctx.nodes_urls.get(&operation_ack.leader_id) {
        debug!(
            leader_id = operation_ack.leader_id,
            leader_url, "updating last known leader url"
        );
        *ctx.pbft_leader_url.lock().unwrap() = leader_url.clone();
    }

    Ok(operation_ack)
}

async fn broadcast_request_to_all(
    ctx: &HandlerContext,
    msg: &ClientRequest,
) -> Result<reqwest::Response, Error> {
    let futs = FuturesUnordered::new();

    for (_, url) in ctx.nodes_urls.iter() {
        let fut = ctx
            .client
            .post(format!("{}/api/v1/consensus/operation", url))
            .json(msg)
            .timeout(Duration::from_secs(5))
            .send();
        futs.push(fut);
    }

    for fut in futs {
        match fut.await {
            Ok(resp) => {
                if resp.status().is_success() {
                    return Ok(resp);
                }
                error!(status = ?resp.status(), "failed to send request to replica");
            }
            Err(err) => {
                error!(error = ?err, "failed to send request to replica");
            }
        }
    }

    Err(Error::FailedToBroadcast)
}

// Consensus external router handlers
async fn handle_consensus_operation_execute(
    ctx: axum::extract::State<HandlerContext>,
    Json(client_request): Json<ClientRequest>,
) -> impl axum::response::IntoResponse {
    match ctx.pbft_module.handle_client_request(client_request).await {
        Ok(ack) => Ok(Json(ack)),
        Err(err) => {
            error!(error = ?err, "failed to handle client request");
            Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response())
        }
    }
}

// Consensus internal router handlers

async fn handle_consensus_pre_prepare(
    ctx: axum::extract::State<HandlerContext>,
    JsonAuthenticatedExt(client_request): JsonAuthenticatedExt<ClientRequestBroadcast>,
) -> impl axum::response::IntoResponse {
    match ctx
        .pbft_module
        .handle_client_request_broadcast(client_request.sender_id, client_request.data)
    {
        Ok(_) => Ok(StatusCode::OK),
        Err(err) => {
            error!(error = ?err, "failed to handle client request broadcast");
            Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response())
        }
    }
}

async fn handle_consensus_message(
    ctx: axum::extract::State<HandlerContext>,
    JsonAuthenticatedExt(protocol_msg): JsonAuthenticatedExt<ProtocolMessageBroadcast>,
) -> impl axum::response::IntoResponse {
    match ctx
        .pbft_module
        .handle_consensus_message(protocol_msg.sender_id, protocol_msg.data.message)
    {
        Ok(_) => Ok(StatusCode::OK),
        Err(err) => {
            error!(error = ?err, "failed to handle consensus request");
            Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response())
        }
    }
}

async fn handle_state_dump(
    ctx: axum::extract::State<HandlerContext>,
) -> impl axum::response::IntoResponse {
    let state = ctx.pbft_module.get_state();

    Json(state)
}

fn get_sender_replica_id(headers_map: &HeaderMap) -> Result<u64, axum::response::Response> {
    headers_map
        .get(pbft_core::api::REPLICA_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or((StatusCode::BAD_REQUEST, "Missing replica ID header").into_response())
}

fn get_replica_signature(headers_map: &HeaderMap) -> Result<String, axum::response::Response> {
    headers_map
        .get(pbft_core::api::REPLICA_SIGNATURE_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string())
        .ok_or((StatusCode::BAD_REQUEST, "Missing replica signature header").into_response())
}

fn get_request_id(headers_map: &HeaderMap) -> Result<uuid::Uuid, axum::response::Response> {
    headers_map
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(uuid::Uuid::parse_str)
        // If client does not specify request ID, we assume it is a new request
        // and generate a new ID.
        .unwrap_or(Ok(uuid::Uuid::new_v4()))
        .map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()).into_response())
}
