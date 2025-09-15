use std::{pin::Pin, time::Duration};
use futures::Future;
use serde::Serialize;
use serde_json::{json, Value};
use tracing::{debug, error};
use crate::replica_api::{FinalizeBlockRequest, GetProposalRequest, JsonRpcRequest, JsonRpcResponse, VerifyProposalRequest};
use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum ReplicaClientError {
    #[error("Failed to send message to {url}, error: {error}")]
    SendError { url: String, error: reqwest::Error },

    #[error("Failed to parse response: {context}, error: {error}")]
    ResponseError {
        context: &'static str,
        error: reqwest::Error,
    },

    #[error("Serde error, context {context}, error: {error}")]
    SerdeError {
        context: &'static str,
        error: serde_json::Error,
    },

    #[error("Unexpected status code when sending message to {url}, status: {status_code}")]
    UnexpectedStatusError {
        url: String,
        status_code: reqwest::StatusCode,
    },

    #[error("Retries limit exceeded when sending message to {url}, attempts: {attempts}, last error: {last_error}")]
    RetriesLimitExceededError {
        url: String,
        attempts: i32,
        last_error: Box<ReplicaClientError>,
    },
}

impl ReplicaClientError {
    pub fn send_error(url: String) -> impl FnOnce(reqwest::Error) -> Self + 'static {
        move |error| Self::SendError { url, error }
    }

    pub fn response_error(context: &'static str) -> impl FnOnce(reqwest::Error) -> Self + 'static {
        move |error| Self::ResponseError { context, error }
    }

    pub fn serde_error(context: &'static str) -> impl FnOnce(serde_json::Error) -> Self + 'static {
        move |error| Self::SerdeError { context, error }
    }
}

#[async_trait]
pub trait ReplicaClientApi: Send + Sync {
    async fn get_proposal(&self, msg: GetProposalRequest) -> Result<Value>;
    async fn verify_proposal(&self, msg: VerifyProposalRequest) -> Result<()>;
    async fn finalize_block(&self, msg: FinalizeBlockRequest) -> Result<()>;
}

type Result<T> = std::result::Result<T, ReplicaClientError>;

pub struct ReplicaClient {
    client: reqwest::Client,
}

impl ReplicaClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    async fn call<M, F>(&self, msg: M, path: &'static str, call_fn: F) -> Result<JsonRpcResponse>
    where
        M: Clone + Serialize + Send + Sync + 'static,
        F: FnOnce(
                reqwest::Client,
                M,
                String,
            ) -> Pin<Box<dyn Future<Output = Result<JsonRpcResponse>> + Send>>
            + Send + 'static,
    {
        let url = format!("http://localhost:8000/{}", path);
        let client = self.client.clone();
        
        call_fn(client, msg, url).await
    }

    pub async fn send_msg<T: Serialize>(
        client: &reqwest::Client,
        method: &str,
        msg: &T,
        url: &str,
    ) -> Result<JsonRpcResponse> {
        let params = serde_json::to_value(msg).map_err(ReplicaClientError::serde_error(
            "failed to serialize request body",
        ))?;

        let json_rpc_request = JsonRpcRequest::new(method, params);
        let body = serde_json::to_vec(&json_rpc_request)
        .map_err(ReplicaClientError::serde_error("failed to serialize JSON-RPC request"))?;

        let res = client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(ReplicaClientError::send_error(url.to_string()))?;

        if res.status().is_success() {
            let json_rpc_response: JsonRpcResponse = res.json()
                .await
                .map_err(ReplicaClientError::response_error("failed to parse JSON-RPC response"))?;
            Ok(json_rpc_response)
        } else {
            Err(ReplicaClientError::UnexpectedStatusError {
                url: url.to_string(),
                status_code: res.status(),
            })
        }
    }

    async fn send_with_retires<T: Serialize + std::fmt::Debug>(
        client: reqwest::Client,
        method: &str,
        msg: &T,
        url: &str,
    ) -> Result<JsonRpcResponse> {
        let mut attempt = 1;
        let mut backoff = Duration::from_millis(200);

        debug!(
            url = url,
            msg = ?msg,
            "attempting to send request",
        );

        loop {
            match ReplicaClient::send_msg(&client, method, msg, url).await {
                Ok(response) => return Ok(response),
                Err(err) => {
                    error!(
                        error = ?err,
                        url = url,
                        method = method,
                        attempt,
                        "failed to send request"
                    );

                    if attempt > 3 {
                        return Err(ReplicaClientError::RetriesLimitExceededError {
                            url: url.to_string(),
                            attempts: attempt,
                            last_error: Box::new(err),
                        });
                    }

                    attempt += 1;
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
    }
}

#[async_trait]
impl ReplicaClientApi for ReplicaClient {
    async fn get_proposal(&self, msg: GetProposalRequest) -> Result<Value> {
        // let response = self.call(
        //     msg,
        //     "api/v1",
        //     |client, msg, url| {
        //         Box::pin(async move {
        //             ReplicaClient::send_with_retires(client, "get_proposal", &msg, &url).await
        //         })
        //     },
        // ).await?;

        // Ok(response.result)

        Ok(json!("test"))
    }

    async fn verify_proposal(&self, msg: VerifyProposalRequest) -> Result<()> {
        let response = self.call(
            msg,
            "api/v1",
            |client, msg, url| {
                Box::pin(async move {
                    ReplicaClient::send_with_retires(client, "verify_proposal", &msg, &url).await
                })
            },
        ).await?;

        if let Some(s) = response.result.as_str() {
            if s == "ok" {
                Ok(())
            } else {
                panic!("response is not ok")
            }
        } else {
            panic!("response is not string")
        }
    }

    async fn finalize_block(&self, msg: FinalizeBlockRequest) -> Result<()> {
        let response = self.call(
            msg,
            "api/v1",
            |client, msg, url| {
                Box::pin(async move {
                    ReplicaClient::send_with_retires(client, "finalize_block", &msg, &url).await
                })
            },
        ).await?;

        if let Some(s) = response.result.as_str() {
            if s == "ok" {
                Ok(())
            } else {
                panic!("response is not ok")
            }
        } else {
            panic!("response is not string")
        }  
    }
}
