mod tunnel;
mod utils;
mod auth;
mod config;

use std::sync::Arc;
use tokio::net::TcpListener;
use anyhow::Result;
use axum::Router;

use crate::auth::services::AuthService;
use crate::config::AppConfig;

use crate::tunnel::interface::ServerTunnel;
use crate::tunnel::ws_server::WebSocketTunnel;

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::from_env();

    let pool = match sqlx::PgPool::connect(&config.database_uri).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to connect to database: {}", e);
            return Err(e.into());
        }
    };

    let auth_service = Arc::new(AuthService::new(pool, config.clone()));

    let app = Router::new()
        .route("/api/auth", axum::routing::post(auth::controllers::login))
        .with_state(auth_service.clone());

    let ws_tunnel = WebSocketTunnel::new(auth_service.clone());
    let ws_listener = ws_tunnel.init(config.host.clone(), config.web_socket_port).await?;

    tokio::spawn(async move {
        let _ = ws_tunnel.run(ws_listener).await;
    });

    let http_listener = TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;
    println!("Auth server running on http://localhost:{}", config.port);
    println!("WebSocket tunnel running on ws://localhost:{}", config.web_socket_port);

    axum::serve(http_listener, app).await?;

    Ok(())
}