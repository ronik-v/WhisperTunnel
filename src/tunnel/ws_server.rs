use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{Bytes, Message, handshake::server::{Request, Response}},
    WebSocketStream,
};

use crate::tunnel::interface::ServerTunnel;
use crate::utils::get_current_date;
use crate::auth::services::AuthService;
use std::sync::Arc;

#[derive(Clone)]
pub struct WebSocketTunnel {
    auth_service: Arc<AuthService>,
}

impl WebSocketTunnel {
    pub fn new(auth_service: Arc<AuthService>) -> Self {
        Self { auth_service }
    }
}

#[async_trait]
impl ServerTunnel for WebSocketTunnel {
    type Error = anyhow::Error;
    type Message = Message;
    type Transport = WebSocketStream<TcpStream>;

    async fn init(&self, host: String, port: u16) -> Result<TcpListener, Self::Error> {
        let addr = format!("{}:{}", host, port);
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to bind WebSocket listener on {}: {}", addr, e))?;

        println!("{} [INFO] WebSocket listener started on {}", get_current_date(), addr);
        Ok(listener)
    }

    async fn run(&self, listener: TcpListener) -> Result<(), Self::Error> {
        println!("{} [INFO] Starting accept loop...", get_current_date());

        loop {
            let (tcp_stream, addr) = match listener.accept().await {
                Ok((s, a)) => (s, a),
                Err(e) => {
                    eprintln!("{} [ERROR] Accept failed: {}", get_current_date(), e);
                    continue;
                }
            };

            println!("{} [INFO] New connection from {}", get_current_date(), addr);

            let tunnel = self.clone();
            tokio::spawn(async move {
                let ws_stream = match accept_hdr_async(tcp_stream, |req: &Request, _response: Response| {
                    if let Some(cookie) = req.headers().get("cookie") {
                        if let Ok(cookie_str) = cookie.to_str() {
                            if let Some(token_part) = cookie_str.split(';').find(|s| s.trim().starts_with("token=")) {
                                let token = token_part.trim().trim_start_matches("token=").trim();
                                if !token.is_empty() {
                                    if let Ok(true) = futures::executor::block_on(tunnel.auth_service.verify_token(token)) {
                                        return Ok(Response::default());
                                    }
                                }
                            }
                        }
                    }
                    Err(
                        Response::builder()
                            .status(401)
                            .body(Some("Missing or invalid token".to_string()))
                            .unwrap()
                    )
                }).await {
                    Ok(ws) => ws,
                    Err(e) => {
                        eprintln!("{} [ERROR] WebSocket handshake failed: {}", get_current_date(), e);
                        return;
                    }
                };

                let _ = tunnel.handle_connection(ws_stream).await;
            });
        }
    }

    async fn handle_connection(&self, transport: Self::Transport) -> Result<(), Self::Error> {
        let (mut ws_write, mut ws_read) = transport.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

        let writer_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if ws_write.send(msg).await.is_err() {
                    break;
                }
            }
        });

        while let Some(msg_result) = ws_read.next().await {
            let msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("{} [ERROR] Read error: {}", get_current_date(), e);
                    break;
                }
            };

            match msg {
                Message::Binary(data) => {
                    let tunnel = self.clone();
                    let tx_clone = tx.clone();

                    tokio::spawn(async move {
                        let _ = tunnel.process_message(data, tx_clone).await;
                    });
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        drop(tx);
        let _ = writer_task.await;

        Ok(())
    }

    async fn process_message(
        &self,
        data: Bytes,
        tx: mpsc::UnboundedSender<Message>,
    ) -> Result<(), Self::Error> {
        if data.len() < 6 {
            return Ok(());
        }

        let target_ip = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let target_port = u16::from_be_bytes([data[4], data[5]]);
        let payload = &data[6..];

        let target_addr = format!(
            "{}.{}.{}.{}:{}",
            (target_ip >> 24) & 0xff,
            (target_ip >> 16) & 0xff,
            (target_ip >> 8) & 0xff,
            target_ip & 0xff,
            target_port
        );

        println!("{} [INFO] Forwarding to {}", get_current_date(), target_addr);

        let mut remote = match TcpStream::connect(&target_addr).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("{} [ERROR] Failed to connect to {}: {}", get_current_date(), target_addr, e);
                return Ok(());
            }
        };

        if !payload.is_empty() {
            if let Err(e) = remote.write_all(payload).await {
                eprintln!("{} [ERROR] Failed to write payload to {}: {}", get_current_date(), target_addr, e);
                return Ok(());
            }
        }

        let (mut remote_read, _remote_write) = remote.into_split();
        let mut buf = [0u8; 8192];

        loop {
            match remote_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if tx.send(Message::Binary(Bytes::copy_from_slice(&buf[..n]))).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("{} [ERROR] Remote read error: {}", get_current_date(), e);
                    break;
                }
            }
        }

        Ok(())
    }
}