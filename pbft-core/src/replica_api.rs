use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Vec<String>,
    #[serde(default)]
    pub id: Value,
}

impl JsonRpcRequest {
    pub fn new(method: &str, msg: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params: vec![msg],
            id: Value::Null,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub result: Value,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub error: Value,
    pub id: Value,
}
