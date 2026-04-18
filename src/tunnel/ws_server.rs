use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{Bytes, Message},
    WebSocketStream,
};

use crate::tunnel::interface::ServerTunnel;
use crate::utils::get_current_date;
use crate::auth::services::AuthService;

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
                let ws_stream = match accept_async(tcp_stream).await {
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

        let streams: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<Bytes>>>> = Arc::new(Mutex::new(HashMap::new()));

        let writer_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if ws_write.send(msg).await.is_err() {
                    break;
                }
            }
        });

        let token = match ws_read.next().await {
            Some(Ok(Message::Text(token))) => token,
            _ => {
                eprintln!("{} [ERROR] First message must be token", get_current_date());
                return Ok(());
            }
        };

        if !self.auth_service.verify_token(&token).await.unwrap_or(false) {
            eprintln!("{} [ERROR] Invalid token", get_current_date());
            return Ok(());
        }

        println!("{} [INFO] Token accepted. Multiplexing ready for production.", get_current_date());

        while let Some(msg_result) = ws_read.next().await {
            let msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("{} [ERROR] WS read error: {}", get_current_date(), e);
                    break;
                }
            };

            if let Message::Binary(data) = msg {
                let tunnel = self.clone();
                let tx_clone = tx.clone();
                let streams_clone = streams.clone();

                tokio::spawn(async move {
                    let _ = tunnel.process_packet(data, tx_clone, streams_clone).await;
                });
            }
        }

        drop(tx);
        let _ = writer_task.await;
        let mut map = streams.lock().await;
        map.clear();

        println!("{} [INFO] WebSocket connection closed, all streams cleaned", get_current_date());
        Ok(())
    }

    async fn process_packet(
        &self,
        data: Bytes,
        tx: mpsc::UnboundedSender<Message>,
        streams: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<Bytes>>>>,
    ) -> Result<(), Self::Error> {
        if data.len() < 5 {
            let _ = tx.send(Message::Binary(Bytes::from_static(b"ERROR: INVALID_HEADER")));
            return Ok(());
        }

        let stream_id = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let cmd = data[4];
        let payload = &data[5..];

        let mut map = streams.lock().await;

        match cmd {
            0 => {
                if payload.len() < 6 {
                    let _ = tx.send(Message::Binary(Bytes::from_static(b"ERROR: CONNECT needs IP:port")));
                    return Ok(());
                }

                let target_ip = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
                let target_port = u16::from_be_bytes([payload[4], payload[5]]);
                let initial_payload = &payload[6..];

                let target_addr = format!(
                    "{}.{}.{}.{}:{}",
                    (target_ip >> 24) & 0xff,
                    (target_ip >> 16) & 0xff,
                    (target_ip >> 8) & 0xff,
                    target_ip & 0xff,
                    target_port
                );

                println!("{} [INFO] [{}] CONNECT → {}", get_current_date(), stream_id, target_addr);

                let mut remote = match TcpStream::connect(&target_addr).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("{} [ERROR] [{}] Connect failed: {}", get_current_date(), stream_id, e);
                        let _ = tx.send(Message::Binary(Bytes::from(format!("ERROR: CONNECT_FAILED {}", e))));
                        return Ok(());
                    }
                };

                let _ = remote.set_nodelay(true);

                if !initial_payload.is_empty() {
                    let _ = remote.write_all(initial_payload).await;
                    let _ = remote.flush().await;
                }

                let (remote_read, remote_write) = remote.into_split();

                let (stream_tx, mut stream_rx) = mpsc::unbounded_channel::<Bytes>();
                map.insert(stream_id, stream_tx);

                let tx_reader = tx.clone();
                let sid = stream_id;
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let mut reader = remote_read;

                    loop {
                        match reader.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                let mut packet = Vec::with_capacity(5 + n);
                                packet.extend_from_slice(&sid.to_be_bytes());
                                packet.push(1); // DATA
                                packet.extend_from_slice(&buf[0..n]);
                                let _ = tx_reader.send(Message::Binary(Bytes::from(packet)));
                            }
                            Err(e) => {
                                eprintln!("{} [ERROR] [{}] Remote read error: {}", get_current_date(), sid, e);
                                break;
                            }
                        }
                    }

                    let mut close_packet = Vec::with_capacity(5);
                    close_packet.extend_from_slice(&sid.to_be_bytes());
                    close_packet.push(2); // CLOSE
                    let _ = tx_reader.send(Message::Binary(Bytes::from(close_packet)));
                });

                tokio::spawn(async move {
                    let mut writer = remote_write;
                    while let Some(data) = stream_rx.recv().await {
                        if let Err(e) = writer.write_all(&data).await {
                            eprintln!("{} [ERROR] [{}] Remote write error: {}", get_current_date(), stream_id, e);
                            break;
                        }
                        let _ = writer.flush().await;
                    }
                });
            }

            1 => {
                if let Some(sender) = map.get(&stream_id) {
                    let _ = sender.send(Bytes::copy_from_slice(payload));
                } else {
                    println!("{} [WARN] [{}] DATA to unknown stream", get_current_date(), stream_id);
                }
            }

            2 => {
                println!("{} [INFO] [{}] CLOSE requested", get_current_date(), stream_id);
                map.remove(&stream_id);
            }

            _ => println!("{} [WARN] Unknown command {} from stream {}", get_current_date(), cmd, stream_id),
        }

        Ok(())
    }
}