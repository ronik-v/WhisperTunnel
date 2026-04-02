use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String
}

#[derive(Serialize, Deserialize)]
pub struct TokenResponse {
    pub token: String,
    pub created_at: DateTime<Utc>
}

#[derive(Serialize, Deserialize)]
pub struct LoginResponse {
    pub username: String,
    pub meta: TokenResponse
}