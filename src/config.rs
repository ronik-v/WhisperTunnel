use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub web_socket_port: u16,
    pub password_salt: String,
    pub token_alphabet: String,
    pub database_uri: String
}

impl AppConfig {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        envy::prefixed("").from_env::<AppConfig>().expect("Fall to load .env")
    }
}