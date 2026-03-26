mod tunnel;
mod utils;

use tokio::net::TcpListener;
use anyhow::{Result, Error};

use crate::tunnel::interface::ServerTunnel;
use crate::tunnel::ws_server::WebSocketTunnel;
use crate::utils::get_current_date;

#[tokio::main]
async fn main() -> Result<()> {
    let host: String = "localhost".to_string();
    let port: u16 = 9090;

    let tunnel: Result<TcpListener, Error> = WebSocketTunnel.init(host, port).await;
    match tunnel {
        Ok(tun) => {
            WebSocketTunnel.run(tun).await?;
        }
        Err(err) => { println!("{} [ERROR] Bad connection - {}", get_current_date(), err) }
    }

    Ok(())
}
