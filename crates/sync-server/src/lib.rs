#![forbid(unsafe_code)]
//! `PostgreSQL` repository, administration, authentication, and Axum routes.

mod admin;
mod auth;
mod config;
mod error;
mod metrics;
mod repository;
mod routes;

pub use admin::{ActorSummary, Admin, AdminError, IssuedCredential};
pub use auth::{hash_secret, AuthContext};
pub use config::Config;
pub use error::ServerError;
pub use metrics::Metrics;
pub use repository::Repository;
pub use routes::{router, AppState};
