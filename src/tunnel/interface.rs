use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{tungstenite::{Bytes, Message}};

#[async_trait]
pub trait ServerTunnel: Send + Sync + 'static {
    type Error: Send + Sync + 'static;
    type Message;
    type Transport;

    async fn init(&self, host: String, port: u16) -> Result<TcpListener, Self::Error>;

    async fn run(&self, listener: TcpListener) -> Result<(), Self::Error>;

    async fn handle_connection(&self, transport: Self::Transport) -> Result<(), Self::Error>;

    async fn process_packet(
        &self,
        data: Bytes,
        tx: mpsc::UnboundedSender<Message>,
        streams: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<Bytes>>>>,
    ) -> Result<(), Self::Error>;
}