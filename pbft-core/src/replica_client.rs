use std::{pin::Pin, time::Duration};
use futures::Future;
use l0::{Blk, Wp};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::process::Command;
use tracing::{debug, error};
use zk::{AsBytes, Fr, Inputs};
use crate::replica_api::{JsonRpcRequest, JsonRpcResponse};
use async_trait::async_trait;
use hex::{encode, decode};

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
    async fn get_proposal(&self) -> Result<Value>;
    async fn verify_proposal(&self, msg: String) -> Result<()>;
    async fn finalize_block(&self, msg: String) -> Result<()>;
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
        let params = Value::Array(vec![json!(msg)]);
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

                    if attempt > 1 {
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
    async fn get_proposal(&self) -> Result<Value> {
        let response = self.call(
            Value::Null,
            "",
            |client, msg, url| {
                Box::pin(async move {
                    ReplicaClient::send_with_retires(client, "get_proposal", &msg, &url).await
                })
            },
        ).await?;

        Ok(response.result)

        // Ok(json!("test"))
    }

    async fn verify_proposal(&self, msg: String) -> Result<()> {
        let response = self.call(
            msg,
            "",
            |client, msg, url| {
                Box::pin(async move {
                    ReplicaClient::send_with_retires(client, "verify_proposal", &msg, &url).await
                })
            },
        ).await?;

        let s = response.result.as_str().unwrap().to_string();
        if s == "ok" {
            Ok(())
        } else {
            panic!("verify_proposal, response is not ok")
        }

        // Ok(())
    }

    async fn finalize_block(&self, msg: String) -> Result<()> {
        let msg = proof(msg);
        let response = self.call(
            msg,
            "",
            |client, msg, url| {
                Box::pin(async move {
                    ReplicaClient::send_with_retires(client, "finalize_block", &msg, &url).await
                })
            },
        ).await?;

        let s = response.result.as_str().unwrap().to_string();
        if s == "ok" {
            Ok(())
        } else {
            panic!("finalize_block, response is not ok")
        }

        // Ok(())
    }
}

fn proof(block: String) -> String {
    let blk_bytes = decode(block).unwrap();
    let blk: Blk = Blk::dec(&mut blk_bytes.into_iter()).unwrap();
    let inputs: Inputs = blk.clone().into();
    let (x, y, z, w): (Fr, Fr, Fr, Fr) = inputs.into();
    let output = tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            Command::new("get_proof_of_permissionless_account")
                .arg(hex::encode(x.enc().collect::<Vec<u8>>()))
                .arg(hex::encode(y.enc().collect::<Vec<u8>>()))
                .arg(hex::encode(z.enc().collect::<Vec<u8>>()))
                .arg(hex::encode(w.enc().collect::<Vec<u8>>()))
                .output().await
        })
    })
    .unwrap();
    println!("PROOF: {:?}", output);
    let proof_hex = String::from_utf8(output.stdout).unwrap();
    let proof_bytes = hex::decode(proof_hex.trim()).unwrap();
    let proof = zk::Proof::dec(&mut proof_bytes.into_iter()).unwrap();
    println!("PROOF DECODED: {:?}", proof);

    let vk: zk::Vk = zk::Vk::dec(&mut hex::decode(&"a68f8e3101a4b6ca051a1b3f4739f6cab2b6355dd6a7a2a093b0282b0b31f74737690d4c2c05083ef5028cb6d1af9757b634210775924e93e45e4a7f44a41860950bd06391326f99185c1002fd3ee3841ea1eedd34805e03bb1694ddf8d6566a931be469fe30cd9b0492cf61efd3c8d06516ab50b868e9b110955f7bd02433771c7dc8d0699d6a104e5d83bfc6b94788a37256ad812131bfb613d10e0192b6b673d9ea8a191782e671e9b7ee3af306713b73bdfd33fec4d6fa4c9b85aa213ff1c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000a9679c273614e91810d738b6e46aef9e4b91366511ec93d3b66e0976048378aa906e67a6174192815006c349211668750000000103").unwrap().into_iter()).unwrap();
    let wp: Wp<Blk> = Wp { vk: vk, proof: proof, val: blk };
    wp.clone().check().unwrap();
    let r: Vec<u8> = wp.enc().collect();
    encode(r)
}