// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Fault injection support for validation scenarios using
//! [Toxiproxy](https://github.com/Shopify/toxiproxy).
//!
//! Toxiproxy is a TCP proxy that simulates network conditions like latency,
//! bandwidth limits, connection resets, and more. This module provides:
//!
//! - [`FaultConfig`] — a builder for declaring faults at the [`Scenario`] level.
//! - [`Toxic`] — strongly-typed representations of each Toxiproxy toxic.
//! - [`ToxiproxyClient`] — an async HTTP client for the Toxiproxy control API.
//!
//! # Architecture
//!
//! When fault injection is enabled via
//! [`Scenario::with_fault_injection`](crate::scenario::Scenario::with_fault_injection),
//! the framework automatically:
//!
//! 1. Starts a Toxiproxy Docker container alongside the validation pipeline.
//! 2. Rewires the data path so traffic flows through the proxy.
//! 3. Configures the requested toxics via the Toxiproxy HTTP API.
//!
//! [`Scenario`]: crate::scenario::Scenario

use crate::container::ContainerConfig;
use crate::error::ValidationError;
use reqwest::Client;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;

/// Default Toxiproxy Docker image.
pub const TOXIPROXY_IMAGE: &str = "ghcr.io/shopify/toxiproxy";
/// Default Toxiproxy Docker image tag.
pub const TOXIPROXY_TAG: &str = "2.12.0";
/// Toxiproxy control API port inside the container.
pub const TOXIPROXY_API_PORT: u16 = 8474;
/// Default proxy listen port inside the Toxiproxy container.
pub(crate) const TOXIPROXY_PROXY_PORT: u16 = 25000;

/// Which link in the data path to inject faults on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FaultTarget {
    /// Faults on the generator -> SUV pipeline link.
    #[default]
    Ingress,
    /// Faults on the SUV pipeline -> capture link.
    Egress,
}

/// Direction of a toxic relative to the proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ToxicDirection {
    /// Affects client -> server traffic.
    Upstream,
    /// Affects server -> client traffic.
    #[default]
    Downstream,
}

/// A network fault to inject via Toxiproxy.
///
/// Each variant maps to a
/// [Toxiproxy toxic type](https://github.com/Shopify/toxiproxy#toxics).
#[derive(Debug, Clone, PartialEq)]
pub enum Toxic {
    /// Add a delay to all data. Delay is `latency_ms` +/- `jitter_ms`.
    Latency {
        /// Time in milliseconds.
        latency_ms: u64,
        /// Jitter in milliseconds.
        jitter_ms: u64,
    },
    /// Limit bandwidth to `rate_kb_per_sec` KB/s.
    Bandwidth {
        /// Rate in KB/s.
        rate_kb_per_sec: u64,
    },
    /// Delay the TCP socket from closing by `delay_ms`.
    SlowClose {
        /// Time in milliseconds.
        delay_ms: u64,
    },
    /// Drop all data and close the connection after `timeout_ms`.
    /// If `timeout_ms` is 0, the connection stays open but data is dropped.
    Timeout {
        /// Time in milliseconds.
        timeout_ms: u64,
    },
    /// Simulate a TCP RST (connection reset by peer) after `timeout_ms`.
    ResetPeer {
        /// Time in milliseconds.
        timeout_ms: u64,
    },
    /// Slice TCP data into small packets with an optional delay between them.
    Slicer {
        /// Average size of each sliced packet in bytes.
        avg_size: u64,
        /// Variation in bytes around `avg_size`.
        size_variation: u64,
        /// Delay between packets in microseconds.
        delay_us: u64,
    },
    /// Close the connection after `bytes` have been transmitted.
    LimitData {
        /// Number of bytes before the connection is closed.
        bytes: u64,
    },
}

impl Toxic {
    /// Create a latency toxic.
    #[must_use]
    pub fn latency(latency_ms: u64, jitter_ms: u64) -> Self {
        Self::Latency {
            latency_ms,
            jitter_ms,
        }
    }

    /// Create a bandwidth toxic.
    #[must_use]
    pub fn bandwidth(rate_kb_per_sec: u64) -> Self {
        Self::Bandwidth { rate_kb_per_sec }
    }

    /// Create a slow_close toxic.
    #[must_use]
    pub fn slow_close(delay_ms: u64) -> Self {
        Self::SlowClose { delay_ms }
    }

    /// Create a timeout toxic.
    #[must_use]
    pub fn timeout(timeout_ms: u64) -> Self {
        Self::Timeout { timeout_ms }
    }

    /// Create a reset_peer toxic.
    #[must_use]
    pub fn reset_peer(timeout_ms: u64) -> Self {
        Self::ResetPeer { timeout_ms }
    }

    /// Create a slicer toxic.
    #[must_use]
    pub fn slicer(avg_size: u64, size_variation: u64, delay_us: u64) -> Self {
        Self::Slicer {
            avg_size,
            size_variation,
            delay_us,
        }
    }

    /// Create a limit_data toxic.
    #[must_use]
    pub fn limit_data(bytes: u64) -> Self {
        Self::LimitData { bytes }
    }

    /// Return the Toxiproxy type string for this toxic.
    fn toxic_type(&self) -> &'static str {
        match self {
            Toxic::Latency { .. } => "latency",
            Toxic::Bandwidth { .. } => "bandwidth",
            Toxic::SlowClose { .. } => "slow_close",
            Toxic::Timeout { .. } => "timeout",
            Toxic::ResetPeer { .. } => "reset_peer",
            Toxic::Slicer { .. } => "slicer",
            Toxic::LimitData { .. } => "limit_data",
        }
    }

    /// Return the toxic-specific attributes as a key-value map.
    fn attributes(&self) -> HashMap<String, u64> {
        let mut attrs = HashMap::new();
        match self {
            Toxic::Latency {
                latency_ms,
                jitter_ms,
            } => {
                let _ = attrs.insert("latency".into(), *latency_ms);
                let _ = attrs.insert("jitter".into(), *jitter_ms);
            }
            Toxic::Bandwidth { rate_kb_per_sec } => {
                let _ = attrs.insert("rate".into(), *rate_kb_per_sec);
            }
            Toxic::SlowClose { delay_ms } => {
                let _ = attrs.insert("delay".into(), *delay_ms);
            }
            Toxic::Timeout { timeout_ms } => {
                let _ = attrs.insert("timeout".into(), *timeout_ms);
            }
            Toxic::ResetPeer { timeout_ms } => {
                let _ = attrs.insert("timeout".into(), *timeout_ms);
            }
            Toxic::Slicer {
                avg_size,
                size_variation,
                delay_us,
            } => {
                let _ = attrs.insert("average_size".into(), *avg_size);
                let _ = attrs.insert("size_variation".into(), *size_variation);
                let _ = attrs.insert("delay".into(), *delay_us);
            }
            Toxic::LimitData { bytes } => {
                let _ = attrs.insert("bytes".into(), *bytes);
            }
        }
        attrs
    }
}

/// A toxic combined with its direction and probability of application.
#[derive(Debug, Clone)]
pub struct ToxicSpec {
    /// The toxic to apply.
    pub toxic: Toxic,
    /// Direction relative to the proxy (upstream or downstream).
    pub direction: ToxicDirection,
    /// Probability of the toxic being applied (0.0-1.0).
    pub toxicity: f32,
}

/// Configuration for fault injection in a validation scenario.
///
/// Use [`FaultConfig::ingress`] or [`FaultConfig::egress`] to create a
/// configuration, then chain [`add_toxic`](Self::add_toxic) calls.
///
/// # Example
///
/// ```
/// use otap_df_validation::fault_injection::{FaultConfig, Toxic, ToxicDirection};
///
/// let config = FaultConfig::ingress()
///     .add_toxic(Toxic::latency(500, 100))
///     .add_toxic_with(Toxic::bandwidth(64), ToxicDirection::Upstream, 0.8);
/// ```
#[derive(Debug, Clone, Default)]
pub struct FaultConfig {
    /// Which link to inject faults on.
    pub(crate) target: FaultTarget,
    /// Toxics to apply.
    pub(crate) toxics: Vec<ToxicSpec>,
}

impl FaultConfig {
    /// Create fault config targeting the ingress link (generator -> SUV pipeline).
    #[must_use]
    pub fn ingress() -> Self {
        Self {
            target: FaultTarget::Ingress,
            toxics: Vec::new(),
        }
    }

    /// Create fault config targeting the egress link (SUV pipeline -> capture).
    #[must_use]
    pub fn egress() -> Self {
        Self {
            target: FaultTarget::Egress,
            toxics: Vec::new(),
        }
    }

    /// Add a toxic with default direction (downstream) and 100% probability.
    #[must_use]
    pub fn add_toxic(mut self, toxic: Toxic) -> Self {
        self.toxics.push(ToxicSpec {
            toxic,
            direction: ToxicDirection::Downstream,
            toxicity: 1.0,
        });
        self
    }

    /// Add a toxic with a custom direction and probability.
    #[must_use]
    pub fn add_toxic_with(
        mut self,
        toxic: Toxic,
        direction: ToxicDirection,
        toxicity: f32,
    ) -> Self {
        self.toxics.push(ToxicSpec {
            toxic,
            direction,
            toxicity,
        });
        self
    }
}

// ---------------------------------------------------------------------------
// Toxiproxy HTTP Client
// ---------------------------------------------------------------------------

/// JSON body for `POST /proxies`.
#[derive(Serialize)]
struct ProxyCreate {
    /// Unique name for this proxy.
    name: String,
    /// Address the proxy listens on (e.g., `"0.0.0.0:25000"`).
    listen: String,
    /// Address to forward traffic to (e.g., `"host.docker.internal:4317"`).
    upstream: String,
    /// Whether the proxy is enabled.
    enabled: bool,
}

/// JSON body for `POST /proxies/{proxy}/toxics`.
#[derive(Serialize)]
struct ToxicCreate {
    /// Unique name for this toxic (e.g., `"latency_downstream"`).
    name: String,
    /// Toxiproxy toxic type (e.g., `"latency"`, `"bandwidth"`).
    r#type: String,
    /// Direction: `"upstream"` or `"downstream"`.
    stream: String,
    /// Probability of the toxic being applied (0.0-1.0).
    toxicity: f32,
    /// Toxic-specific configuration attributes.
    attributes: HashMap<String, u64>,
}

/// Async HTTP client for the Toxiproxy control API.
///
/// This is a thin wrapper around [`reqwest::Client`] that targets the
/// Toxiproxy REST endpoints. It is intentionally not a public API —
/// callers interact with [`FaultConfig`] at the [`Scenario`] level.
///
/// [`Scenario`]: crate::scenario::Scenario
pub(crate) struct ToxiproxyClient {
    base_url: String,
    client: Client,
}

impl ToxiproxyClient {
    /// Create a new client pointing at the given Toxiproxy HTTP base URL
    /// (e.g., `"http://127.0.0.1:8474"`).
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: Client::new(),
        }
    }

    /// Poll `GET /version` until Toxiproxy is ready or `max_attempts` is
    /// exhausted.
    pub async fn wait_until_ready(
        &self,
        max_attempts: usize,
        backoff: Duration,
    ) -> Result<(), ValidationError> {
        for _ in 0..max_attempts {
            if self.is_running().await {
                return Ok(());
            }
            sleep(backoff).await;
        }
        Err(ValidationError::FaultInjection(
            "Toxiproxy did not become ready".into(),
        ))
    }

    /// Check whether Toxiproxy is responding to `GET /version`.
    pub async fn is_running(&self) -> bool {
        self.client
            .get(format!("{}/version", self.base_url))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    /// Create a new proxy via `POST /proxies`.
    pub async fn create_proxy(
        &self,
        name: &str,
        listen: &str,
        upstream: &str,
    ) -> Result<(), ValidationError> {
        let body = ProxyCreate {
            name: name.into(),
            listen: listen.into(),
            upstream: upstream.into(),
            enabled: true,
        };
        let _ = self
            .client
            .post(format!("{}/proxies", self.base_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| ValidationError::FaultInjection(format!("create proxy: {e}")))?
            .error_for_status()
            .map_err(|e| ValidationError::FaultInjection(format!("create proxy: {e}")))?;
        Ok(())
    }

    /// Add a toxic to an existing proxy via `POST /proxies/{proxy}/toxics`.
    pub async fn add_toxic(
        &self,
        proxy_name: &str,
        toxic: &Toxic,
        direction: ToxicDirection,
        toxicity: f32,
    ) -> Result<(), ValidationError> {
        let stream = match direction {
            ToxicDirection::Upstream => "upstream",
            ToxicDirection::Downstream => "downstream",
        };
        let body = ToxicCreate {
            name: format!("{}_{}", toxic.toxic_type(), stream),
            r#type: toxic.toxic_type().into(),
            stream: stream.into(),
            toxicity,
            attributes: toxic.attributes(),
        };
        let _ = self
            .client
            .post(format!("{}/proxies/{}/toxics", self.base_url, proxy_name))
            .json(&body)
            .send()
            .await
            .map_err(|e| ValidationError::FaultInjection(format!("add toxic: {e}")))?
            .error_for_status()
            .map_err(|e| ValidationError::FaultInjection(format!("add toxic: {e}")))?;
        Ok(())
    }

    /// Enable all proxies and remove all active toxics via `POST /reset`.
    #[allow(dead_code)]
    pub async fn reset(&self) -> Result<(), ValidationError> {
        let _ = self
            .client
            .post(format!("{}/reset", self.base_url))
            .send()
            .await
            .map_err(|e| ValidationError::FaultInjection(format!("reset: {e}")))?
            .error_for_status()
            .map_err(|e| ValidationError::FaultInjection(format!("reset: {e}")))?;
        Ok(())
    }
}

/// Build a [`ContainerConfig`] for Toxiproxy with the given host port
/// mappings.
///
/// - `api_host_port` is mapped to the Toxiproxy HTTP API port (8474).
/// - `proxy_host_port` is the host port mapped to `proxy_container_port`
///   inside the container.
pub(crate) fn toxiproxy_container(
    api_host_port: u16,
    proxy_host_port: u16,
    proxy_container_port: u16,
) -> ContainerConfig {
    let mut config = ContainerConfig::new(TOXIPROXY_IMAGE, TOXIPROXY_TAG);
    let _ = config
        .mapped_ports
        .insert(TOXIPROXY_API_PORT, api_host_port);
    let _ = config
        .mapped_ports
        .insert(proxy_container_port, proxy_host_port);
    config
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toxic_latency_attributes() {
        let t = Toxic::latency(500, 100);
        let attrs = t.attributes();
        assert_eq!(attrs.get("latency"), Some(&500));
        assert_eq!(attrs.get("jitter"), Some(&100));
        assert_eq!(t.toxic_type(), "latency");
    }

    #[test]
    fn toxic_bandwidth_attributes() {
        let t = Toxic::bandwidth(64);
        let attrs = t.attributes();
        assert_eq!(attrs.get("rate"), Some(&64));
        assert_eq!(t.toxic_type(), "bandwidth");
    }

    #[test]
    fn toxic_slow_close_attributes() {
        let t = Toxic::slow_close(500);
        let attrs = t.attributes();
        assert_eq!(attrs.get("delay"), Some(&500));
        assert_eq!(t.toxic_type(), "slow_close");
    }

    #[test]
    fn toxic_timeout_attributes() {
        let t = Toxic::timeout(5000);
        let attrs = t.attributes();
        assert_eq!(attrs.get("timeout"), Some(&5000));
        assert_eq!(t.toxic_type(), "timeout");
    }

    #[test]
    fn toxic_reset_peer_attributes() {
        let t = Toxic::reset_peer(1000);
        let attrs = t.attributes();
        assert_eq!(attrs.get("timeout"), Some(&1000));
        assert_eq!(t.toxic_type(), "reset_peer");
    }

    #[test]
    fn toxic_slicer_attributes() {
        let t = Toxic::slicer(1024, 128, 500);
        let attrs = t.attributes();
        assert_eq!(attrs.get("average_size"), Some(&1024));
        assert_eq!(attrs.get("size_variation"), Some(&128));
        assert_eq!(attrs.get("delay"), Some(&500));
        assert_eq!(t.toxic_type(), "slicer");
    }

    #[test]
    fn toxic_limit_data_attributes() {
        let t = Toxic::limit_data(2048);
        let attrs = t.attributes();
        assert_eq!(attrs.get("bytes"), Some(&2048));
        assert_eq!(t.toxic_type(), "limit_data");
    }

    #[test]
    fn fault_config_ingress_builder() {
        let cfg = FaultConfig::ingress()
            .add_toxic(Toxic::latency(100, 10))
            .add_toxic_with(Toxic::bandwidth(64), ToxicDirection::Upstream, 0.5);
        assert_eq!(cfg.target, FaultTarget::Ingress);
        assert_eq!(cfg.toxics.len(), 2);
        assert_eq!(cfg.toxics[0].direction, ToxicDirection::Downstream);
        assert!((cfg.toxics[0].toxicity - 1.0).abs() < f32::EPSILON);
        assert_eq!(cfg.toxics[1].direction, ToxicDirection::Upstream);
        assert!((cfg.toxics[1].toxicity - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn fault_config_egress_builder() {
        let cfg = FaultConfig::egress().add_toxic(Toxic::timeout(5000));
        assert_eq!(cfg.target, FaultTarget::Egress);
        assert_eq!(cfg.toxics.len(), 1);
    }

    #[test]
    fn toxiproxy_container_sets_ports() {
        let config = toxiproxy_container(18474, 25001, 25000);
        assert_eq!(config.image, TOXIPROXY_IMAGE);
        assert_eq!(config.tag, TOXIPROXY_TAG);
        assert_eq!(config.mapped_ports.get(&TOXIPROXY_API_PORT), Some(&18474));
        assert_eq!(config.mapped_ports.get(&25000), Some(&25001));
    }
}
