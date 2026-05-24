//! Thin wrapper around `reqwest::Client` for upstream calls.
//!
//! PR3 split out the old `ProviderAdapter` plumbing — URL building and
//! auth header construction now happen at the call site (via
//! `VendorRegistry::resolve` + `VendorExtension::{auth_headers,
//! build_url}`). `ProxyClient` is intentionally adapter-agnostic: it
//! takes a fully-built URL and a ready-to-send header map and just
//! issues the HTTP call.

use anyhow::Result;
use reqwest::header::HeaderMap;
use serde_json::Value;

pub struct ProxyClient {
    pub http: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
#[error("error decoding response body: {source}")]
pub struct UpstreamResponseDecodeError {
    pub source: serde_json::Error,
    pub status: u16,
    pub headers: HeaderMap,
    pub body: bytes::Bytes,
}

impl UpstreamResponseDecodeError {
    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

impl ProxyClient {
    pub fn new(http: reqwest::Client) -> Self {
        Self { http }
    }

    pub async fn call_non_stream(
        &self,
        url: &str,
        headers: HeaderMap,
        body: Value,
    ) -> Result<(Value, u16, HeaderMap)> {
        let resp = self
            .http
            .post(url)
            .headers(headers)
            .json(&body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let resp_headers = resp.headers().clone();
        let bytes = resp.bytes().await?;
        let json: Value =
            serde_json::from_slice(&bytes).map_err(|source| UpstreamResponseDecodeError {
                source,
                status,
                headers: resp_headers.clone(),
                body: bytes,
            })?;
        Ok((json, status, resp_headers))
    }

    pub async fn call_stream(
        &self,
        url: &str,
        headers: HeaderMap,
        body: Value,
    ) -> Result<(reqwest::Response, u16)> {
        let resp = self
            .http
            .post(url)
            .headers(headers)
            .json(&body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        Ok((resp, status))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn serve_once(response: &'static [u8]) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut buf = [0_u8; 2048];
            let _ = socket.read(&mut buf).await.expect("read request");
            socket.write_all(response).await.expect("write response");
        });
        format!("http://{addr}/v1beta/models/gemini:generateContent?key=secret")
    }

    #[tokio::test]
    async fn non_stream_json_decode_error_retains_upstream_metadata() {
        let url = serve_once(
            b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\nx-request-id: upstream-123\r\ncontent-length: 16\r\n\r\nnot valid json!!",
        )
        .await;
        let client = ProxyClient::new(reqwest::Client::new());

        let err = client
            .call_non_stream(
                &url,
                HeaderMap::new(),
                serde_json::json!({"model": "gemini"}),
            )
            .await
            .expect_err("invalid upstream JSON must fail");

        let decode = err
            .downcast_ref::<UpstreamResponseDecodeError>()
            .expect("decode failure should expose upstream status, headers, and raw body");
        assert_eq!(decode.status, 200);
        assert_eq!(
            decode
                .headers
                .get("x-request-id")
                .and_then(|v| v.to_str().ok()),
            Some("upstream-123")
        );
        assert_eq!(decode.body_text(), "not valid json!!");
    }
}
