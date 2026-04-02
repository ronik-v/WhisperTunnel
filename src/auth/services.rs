use anyhow::Result;
use rand::prelude::IndexedRandom;
use sha2::{Sha256, Digest};
use sqlx::PgPool;

use crate::config::AppConfig;
use crate::auth::responses::{LoginRequest, TokenResponse, LoginResponse};

pub struct AuthService {
    pool: PgPool,
    config: AppConfig,
}

impl AuthService {
    pub fn new(pool: PgPool, config: AppConfig) -> Self {
        Self { pool, config }
    }

    pub async fn login(&self, req: LoginRequest) -> Result<LoginResponse> {
        let salted = format!("{}{}", self.config.password_salt, req.password);
        let mut hasher = Sha256::new();
        hasher.update(salted.as_bytes());
        let hash = hasher.finalize();
        let hash_hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();

        let user: Option<(i64, String)> = sqlx::query_as(
            "SELECT id, username FROM users WHERE username = $1 AND password = $2"
        )
            .bind(&req.username)
            .bind(&hash_hex)
            .fetch_optional(&self.pool)
            .await?;

        let (user_id, username) = user.ok_or_else(|| anyhow::anyhow!("Invalid username or password"))?;

        let token = self.generate_token();

        sqlx::query(
            "INSERT INTO user_tokens (user_id, token) VALUES ($1, $2)"
        )
            .bind(user_id)
            .bind(&token)
            .execute(&self.pool)
            .await?;

        let now = chrono::Utc::now();

        Ok(LoginResponse {
            username,
            meta: TokenResponse {
                token,
                created_at: now,
            },
        })
    }

    pub async fn verify_token(&self, token: &str) -> Result<bool> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM user_tokens WHERE token = $1)"
        )
            .bind(token)
            .fetch_one(&self.pool)
            .await?;

        Ok(exists)
    }

    fn generate_token(&self) -> String {
        let alphabet: Vec<char> = self.config.token_alphabet.chars().collect();
        let mut rng = rand::thread_rng();
        (0..24)
            .map(|_| *alphabet.choose(&mut rng).unwrap())
            .collect()
    }
}