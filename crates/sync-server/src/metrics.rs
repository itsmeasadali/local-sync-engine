use std::sync::Arc;

use prometheus_client::{
    encoding::text::encode,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};

#[derive(Clone)]
pub struct Metrics {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Registry,
    requests: Family<Vec<(String, String)>, Counter>,
    accepted: Counter,
    rejected: Counter,
    ready: Gauge,
}

impl Metrics {
    #[must_use]
    pub fn new() -> Self {
        let requests = Family::default();
        let accepted = Counter::default();
        let rejected = Counter::default();
        let ready = Gauge::default();
        let mut registry = Registry::default();
        registry.register(
            "sync_http_requests",
            "HTTP requests by route and status",
            requests.clone(),
        );
        registry.register(
            "sync_mutations_accepted",
            "Newly accepted mutations",
            accepted.clone(),
        );
        registry.register(
            "sync_mutations_rejected",
            "Rejected mutations",
            rejected.clone(),
        );
        registry.register("sync_ready", "Database readiness", ready.clone());
        Self {
            inner: Arc::new(Inner {
                registry,
                requests,
                accepted,
                rejected,
                ready,
            }),
        }
    }

    pub fn request(&self, route: &str, status: u16) {
        self.inner
            .requests
            .get_or_create(&vec![
                ("route".into(), route.into()),
                ("status".into(), status.to_string()),
            ])
            .inc();
    }
    pub fn accepted(&self) {
        self.inner.accepted.inc();
    }
    pub fn rejected(&self) {
        self.inner.rejected.inc();
    }
    pub fn set_ready(&self, ready: bool) {
        self.inner.ready.set(i64::from(ready));
    }
    /// Encodes the registry using the Prometheus/OpenMetrics text format.
    ///
    /// # Errors
    ///
    /// Returns a formatting error if metric encoding fails.
    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut output = String::new();
        encode(&mut output, &self.inner.registry)?;
        Ok(output)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
