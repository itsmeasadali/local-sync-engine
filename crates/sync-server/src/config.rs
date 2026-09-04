use std::{env, net::SocketAddr, time::Duration};

use anyhow::{bail, Context};

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub listen_addr: SocketAddr,
    pub max_batch_size: usize,
    pub max_pull_limit: u32,
    pub max_body_bytes: usize,
    pub max_future_skew: Duration,
    pub request_timeout: Duration,
    pub bootstrap_workspace: Option<String>,
    pub bootstrap_actor: Option<String>,
    pub bootstrap_token: Option<String>,
}

impl Config {
    /// Parses and validates server configuration from `SYNC_*` variables.
    ///
    /// # Errors
    ///
    /// Returns an error for missing required values, parse failures, or
    /// non-positive limits.
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url =
            env::var("SYNC_DATABASE_URL").context("SYNC_DATABASE_URL is required")?;
        let listen_addr = env::var("SYNC_LISTEN_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8080".into())
            .parse()
            .context("invalid SYNC_LISTEN_ADDR")?;
        let max_batch_size = parse("SYNC_MAX_BATCH_SIZE", 100_usize)?;
        let max_pull_limit = parse("SYNC_MAX_PULL_LIMIT", 500_u32)?;
        let max_body_bytes = parse("SYNC_MAX_BODY_BYTES", 2_097_152_usize)?;
        let max_future_skew = Duration::from_secs(parse("SYNC_MAX_FUTURE_SKEW_SECONDS", 300_u64)?);
        let request_timeout = Duration::from_secs(parse("SYNC_REQUEST_TIMEOUT_SECONDS", 30_u64)?);
        if max_batch_size == 0
            || max_pull_limit == 0
            || max_body_bytes == 0
            || request_timeout.is_zero()
        {
            bail!("configured limits and timeout must be positive");
        }
        Ok(Self {
            database_url,
            listen_addr,
            max_batch_size,
            max_pull_limit,
            max_body_bytes,
            max_future_skew,
            request_timeout,
            bootstrap_workspace: env::var("SYNC_BOOTSTRAP_WORKSPACE").ok(),
            bootstrap_actor: env::var("SYNC_BOOTSTRAP_ACTOR").ok(),
            bootstrap_token: env::var("SYNC_BOOTSTRAP_TOKEN").ok(),
        })
    }
}

fn parse<T>(name: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match env::var(name) {
        Ok(value) => value.parse().with_context(|| format!("invalid {name}")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("cannot read {name}")),
    }
}
