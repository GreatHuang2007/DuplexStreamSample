use futures::{Stream, StreamExt};
use std::{pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming, transport::Server};

pub mod pb {
    tonic::include_proto!("streamcalls");
}

use pb::stream_service_server::{StreamService, StreamServiceServer};
use pb::*;

/// 服务端实现：每个连接一个独立的 Echo/Compute 处理逻辑
#[derive(Default)]
pub struct MyStreamService {
    token: String,
}

#[tonic::async_trait]
impl StreamService for MyStreamService {
    type DuplexStreamStream =
        Pin<Box<dyn Stream<Item = Result<StreamResponse, Status>> + Send + 'static>>;

    async fn duplex_stream(
        &self,
        request: Request<Streaming<StreamRequest>>,
    ) -> Result<Response<Self::DuplexStreamStream>, Status> {
        if request
            .metadata()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
            != self.token
        {
            Err(Status::permission_denied("Invalid auth token"))?;
        }

        let peer = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "<unknown>".into());
        tracing::info!("client connected: {peer}");

        let mut inbound = request.into_inner();

        // 出站通道：所有响应/通知都从这里汇入到一条流
        let (tx, rx) = mpsc::channel::<Result<StreamResponse, Status>>(64);

        // ---- 1. 服务端主动通知（心跳示例）------------------------------------
        let notify_tx = tx.clone();
        let notify_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(5));
            ticker.tick().await; // 丢弃第一次立即触发的 tick
            loop {
                ticker.tick().await;
                let msg = StreamResponse {
                    req_id: 0,
                    payload: Some(stream_response::Payload::Notification(Notification {
                        content: format!("heartbeat @ {:?}", std::time::SystemTime::now()),
                    })),
                };
                if notify_tx.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
            tracing::debug!("notifier task exited");
        });

        // ---- 2. 请求处理循环 -------------------------------------------------
        let req_tx = tx.clone();
        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                match item {
                    Ok(req) => {
                        // 每个请求独立 spawn，允许乱序返回（客户端已用 req_id 匹配）
                        let tx = req_tx.clone();
                        tracing::info!("Received request: {:?}", req);
                        tokio::spawn(async move {
                            match handle_request(req).await {
                                Ok(resp) => {
                                    if tx.send(Ok(resp)).await.is_err() {
                                        // 客户端已断连，静默丢弃
                                    }
                                }
                                Err(status) => {
                                    let _ = tx.send(Err(status)).await;
                                }
                            }
                        });
                    }
                    Err(status) => {
                        tracing::warn!("inbound stream error: {status}");
                        let _ = req_tx.send(Err(status)).await;
                        break;
                    }
                }
            }

            // 客户端结束流：中止通知任务，随后 req_tx 也会被 drop，rx 自然关闭
            notify_handle.abort();
            tracing::info!("client stream closed, notifier aborted");
        });

        // 主线程不再持有 tx，否则 rx 永远不会结束
        drop(tx);

        let outbound = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(outbound)))
    }
}

/// 处理单个请求，返回对应的响应（不关心发送，让调用方决定）
async fn handle_request(req: StreamRequest) -> Result<StreamResponse, Status> {
    let req_id = req.req_id;
    let payload = req
        .payload
        .ok_or_else(|| Status::invalid_argument("empty request payload"))?;

    let resp = match payload {
        stream_request::Payload::Echo(e) => StreamResponse {
            req_id,
            payload: Some(stream_response::Payload::Echo(EchoResponse {
                message: format!("echo: {}", e.message),
            })),
        },
        stream_request::Payload::SlowCompute(c) => {
            // 模拟耗时的服务端计算，验证乱序返回能力
            let n = c.n as u64;
            tokio::time::sleep(Duration::from_millis(300 * n)).await;
            let result = n.saturating_mul(n + 1) / 2; // 1+2+...+n
            StreamResponse {
                req_id,
                payload: Some(stream_response::Payload::SlowCompute(SlowComputeResponse {
                    result,
                })),
            }
        }
    };

    Ok(resp)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();

    let addr = "[::1]:50051".parse()?;
    tracing::info!("StreamService listening on {addr}");

    Server::builder()
        .add_service(StreamServiceServer::new(MyStreamService {
            token: "abc".to_string(),
        }))
        .serve(addr)
        .await?;

    Ok(())
}
