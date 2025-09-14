#[cfg(test)]
use std::time::Duration;
use std::{
    net::SocketAddr,
    sync::{Arc},
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
use pbft_core::{
    api::{ProposeBlockMsgBroadcast, ProtocolMessageBroadcast}
};
use serde::{de::DeserializeOwned};
use tracing::{error, info};

use crate::{
    config::AppConfig
};

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
        });

        Self {
            addr,
            ctx: api_context,
        }
    }

    pub async fn run(&mut self, mut rx_shutdown: tokio::sync::broadcast::Receiver<()>) {
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

// Consensus internal router handlers
async fn handle_consensus_pre_prepare(
    ctx: axum::extract::State<HandlerContext>,
    JsonAuthenticatedExt(request): JsonAuthenticatedExt<ProposeBlockMsgBroadcast>,
) -> impl axum::response::IntoResponse {
    match ctx
        .pbft_module
        .handle_propose_block_broadcast(request.sender_id, request.data)
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
