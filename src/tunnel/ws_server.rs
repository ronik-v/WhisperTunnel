use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

use crate::tunnel::interface::ServerTunnel;
use crate::utils::get_current_date;

#[derive(Clone)]
pub struct WebSocketTunnel;

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

                if let Err(e) = tunnel.handle_connection(ws_stream).await {
                    eprintln!("{} [ERROR] Connection handler failed for {}: {}",
                              get_current_date(), addr, e);
                }
            });
        }
    }

    async fn handle_connection(&self, transport: Self::Transport) -> Result<(), Self::Error> {
        let (mut write, mut read) = transport.split();

        while let Some(msg_result) = read.next().await {
            let msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("{} [ERROR] Read error: {}", get_current_date(), e);
                    break;
                }
            };

            self.process_message(msg.clone()).await?;

            if let Err(e) = write.send(msg).await {
                eprintln!("{} [ERROR] Write error: {}", get_current_date(), e);
                break;
            }
        }

        Ok(())
    }

    async fn process_message(&self, msg: Self::Message) -> Result<Option<Self::Message>, Self::Error> {
        match &msg {
            Message::Text(text) => println!("{} [INFO] Received text: {}", get_current_date(), text),
            Message::Binary(data) => println!("{} [INFO] Received binary: {} bytes", get_current_date(), data.len()),
            Message::Close(_) => println!("{} [INFO] Client closed connection", get_current_date()),
            _ => {}
        }
        Ok(Some(msg))
    }
}