use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;
use std::sync::Arc;

use crate::auth::services::AuthService;
use crate::auth::responses::LoginRequest;

pub async fn login(
    State(service): State<Arc<AuthService>>,
    Json(payload): Json<LoginRequest>,
) -> impl IntoResponse {
    match service.login(payload).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(_) => (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Invalid credentials"})),
        ).into_response(),
    }
}