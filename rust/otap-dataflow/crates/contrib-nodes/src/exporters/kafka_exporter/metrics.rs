// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry metrics for the Kafka exporter.
//!
//! These metrics are exposed via the OTAP telemetry system and can be queried
//! from the data-plane admin `/api/v1/metrics` endpoint. They follow the standard
//! `metric_set` pattern used by other OTAP nodes.

<<<<<<< HEAD
use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry_macros::metric_set;

/// Metrics for the Kafka exporter.
///
/// Tracks success and failure counts for each signal type (logs, metrics, traces).
///
/// Metric set name: `gateway.exporter.kafka.pdata.metrics`
///
/// Individual metric names (after field name translation `_` → `.`):
/// - `logs.exported` / `logs.failed`
/// - `metrics.exported` / `metrics.failed`
/// - `traces.exported` / `traces.failed`
/// - `acks.received`
/// - `nacks.received`
#[metric_set(name = "gateway.exporter.kafka.pdata.metrics")]
#[derive(Debug, Default, Clone)]
pub struct KafkaExporterMetrics {
    /// Number of log records successfully exported to Kafka.
    #[metric(unit = "{log}")]
    pub logs_exported: Counter<u64>,
    /// Number of log records that failed to export to Kafka.
    #[metric(unit = "{log}")]
    pub logs_failed: Counter<u64>,
    /// Number of metric data points successfully exported to Kafka.
    #[metric(unit = "{datapoint}")]
    pub metrics_exported: Counter<u64>,
    /// Number of metric data points that failed to export to Kafka.
    #[metric(unit = "{datapoint}")]
    pub metrics_failed: Counter<u64>,
    /// Number of trace spans successfully exported to Kafka.
    #[metric(unit = "{span}")]
    pub traces_exported: Counter<u64>,
    /// Number of trace spans that failed to export to Kafka.
    #[metric(unit = "{span}")]
    pub traces_failed: Counter<u64>,
=======
use otap_df_engine::context::PipelineContext;
use otap_df_telemetry::common_attributes::{Outcome, SignalOutcomeAttributes};
use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry::metrics::{MeasurementMetricSet, MetricSet};
use otap_df_telemetry::reporter::MetricsReporter;
use otap_df_telemetry_macros::metric_set;

/// Signal-specific export completion metrics.
#[metric_set(
    name = "exporter.kafka.exports",
    measurement_attributes = SignalOutcomeAttributes
)]
#[derive(Debug, Default, Clone)]
pub struct KafkaExporterExportMetrics {
    /// Number of exported messages partitioned by `signal` and `outcome` (`success` or `failure`).
    #[metric(unit = "{message}")]
    pub messages: Counter<u64>,
}

/// Operational metrics for the Kafka exporter.
#[metric_set(name = "exporter.kafka")]
#[derive(Debug, Default, Clone)]
pub struct KafkaExporterOperationalMetrics {
>>>>>>> main
    /// Number of acks received from downstream.
    #[metric(unit = "{batch}")]
    pub acks_received: Counter<u64>,
    /// Number of nacks received from downstream.
    #[metric(unit = "{batch}")]
    pub nacks_received: Counter<u64>,
    /// Batches where topic was resolved from a transport header.
    #[metric(unit = "{batch}")]
    pub topic_from_header: Counter<u64>,
    /// Batches where topic was resolved from static per-signal config.
    #[metric(unit = "{batch}")]
    pub topic_from_static_config: Counter<u64>,
}

<<<<<<< HEAD
impl KafkaExporterMetrics {
    /// Increments the exported counter for the given signal type.
    pub fn inc_exported(&mut self, signal_type: otap_df_config::SignalType) {
        match signal_type {
            otap_df_config::SignalType::Logs => self.logs_exported.inc(),
            otap_df_config::SignalType::Metrics => self.metrics_exported.inc(),
            otap_df_config::SignalType::Traces => self.traces_exported.inc(),
        }
    }

    /// Increments the failed counter for the given signal type.
    pub fn inc_failed(&mut self, signal_type: otap_df_config::SignalType) {
        match signal_type {
            otap_df_config::SignalType::Logs => self.logs_failed.inc(),
            otap_df_config::SignalType::Metrics => self.metrics_failed.inc(),
            otap_df_config::SignalType::Traces => self.traces_failed.inc(),
        }
    }

    /// Increment ack counter when downstream confirms a batch.
    pub fn inc_ack(&mut self) {
        self.acks_received.inc();
    }

    /// Increment nack counter when downstream rejects a batch.
    pub fn inc_nack(&mut self) {
        self.nacks_received.inc();
    }

    /// Increment counter when topic was resolved from a transport header.
    pub fn inc_topic_from_header(&mut self) {
        self.topic_from_header.inc();
    }

    /// Increment counter when topic was resolved from static per-signal config.
    pub fn inc_topic_from_static_config(&mut self) {
        self.topic_from_static_config.inc();
=======
/// Composite metrics for the Kafka exporter.
pub struct KafkaExporterMetrics {
    /// Metrics related to export outcomes.
    pub export_metrics: MeasurementMetricSet<KafkaExporterExportMetrics>,
    /// Operational metrics for the Kafka exporter.
    pub operational_metrics: MetricSet<KafkaExporterOperationalMetrics>,
}

impl KafkaExporterMetrics {
    /// Registers the metrics for the Kafka exporter.
    pub fn register(pipeline_ctx: &PipelineContext) -> Self {
        Self {
            export_metrics: KafkaExporterExportMetrics::register(pipeline_ctx),
            operational_metrics: KafkaExporterOperationalMetrics::register(pipeline_ctx),
        }
    }

    /// Reports the current metrics to the provided reporter.
    pub fn report(
        &mut self,
        reporter: &mut MetricsReporter,
    ) -> Result<(), otap_df_telemetry::error::Error> {
        reporter
            .report(&mut self.operational_metrics)
            .and_then(|()| reporter.report_measurement(&mut self.export_metrics))
    }

    /// Retrieves the terminal snapshots of the metrics.
    pub fn terminal_snapshots(&mut self) -> Vec<otap_df_telemetry::metrics::MetricSetSnapshot> {
        let mut snapshots = self.operational_metrics.terminal_snapshots();
        snapshots.extend(self.export_metrics.terminal_snapshots());
        snapshots
    }

    /// Increments the counter for successfully exported messages.
    pub fn inc_exported(&mut self, signal: otap_df_config::SignalType) {
        self.export_metrics
            .with(SignalOutcomeAttributes {
                signal,
                outcome: Outcome::Success,
            })
            .messages
            .inc();
    }

    /// Increments the counter for failed export attempts.
    pub fn inc_failed(&mut self, signal: otap_df_config::SignalType) {
        self.export_metrics
            .with(SignalOutcomeAttributes {
                signal,
                outcome: Outcome::Failure,
            })
            .messages
            .inc();
    }

    /// Increments the counter for acks received from downstream.
    pub fn inc_ack(&mut self) {
        self.operational_metrics.acks_received.inc();
    }

    /// Increments the counter for nacks received from downstream.
    pub fn inc_nack(&mut self) {
        self.operational_metrics.nacks_received.inc();
    }

    /// Increments the counter for batches where the topic was resolved from a header.
    pub fn inc_topic_from_header(&mut self) {
        self.operational_metrics.topic_from_header.inc();
    }

    /// Increments the counter for batches where the topic was resolved from static configuration.
    pub fn inc_topic_from_static_config(&mut self) {
        self.operational_metrics.topic_from_static_config.inc();
>>>>>>> main
    }
}

#[cfg(test)]
mod tests {
    use super::*;
<<<<<<< HEAD
    use otap_df_config::SignalType;

    #[test]
    fn inc_exported_traces() {
        let mut m = KafkaExporterMetrics::default();
        m.inc_exported(SignalType::Traces);
        m.inc_exported(SignalType::Traces);
        assert_eq!(m.traces_exported.get(), 2);
        assert_eq!(m.logs_exported.get(), 0);
        assert_eq!(m.metrics_exported.get(), 0);
    }

    #[test]
    fn inc_exported_metrics() {
        let mut m = KafkaExporterMetrics::default();
        m.inc_exported(SignalType::Metrics);
        assert_eq!(m.metrics_exported.get(), 1);
    }

    #[test]
    fn inc_exported_logs() {
        let mut m = KafkaExporterMetrics::default();
        m.inc_exported(SignalType::Logs);
        assert_eq!(m.logs_exported.get(), 1);
    }

    #[test]
    fn inc_failed_traces() {
        let mut m = KafkaExporterMetrics::default();
        m.inc_failed(SignalType::Traces);
        assert_eq!(m.traces_failed.get(), 1);
        assert_eq!(m.traces_exported.get(), 0);
    }

    #[test]
    fn inc_failed_metrics() {
        let mut m = KafkaExporterMetrics::default();
        m.inc_failed(SignalType::Metrics);
        assert_eq!(m.metrics_failed.get(), 1);
    }

    #[test]
    fn inc_failed_logs() {
        let mut m = KafkaExporterMetrics::default();
        m.inc_failed(SignalType::Logs);
        assert_eq!(m.logs_failed.get(), 1);
    }

    #[test]
    fn inc_ack_and_nack() {
        let mut m = KafkaExporterMetrics::default();
        m.inc_ack();
        m.inc_ack();
        m.inc_nack();
        assert_eq!(m.acks_received.get(), 2);
        assert_eq!(m.nacks_received.get(), 1);
    }

    #[test]
    fn counters_are_independent() {
        let mut m = KafkaExporterMetrics::default();
=======
    use crate::exporters::kafka_exporter::exporter::test_support::pipeline_context;
    use otap_df_config::SignalType;

    fn new_metrics() -> KafkaExporterMetrics {
        KafkaExporterMetrics::register(&pipeline_context())
    }

    /// Scenario: Traces are exported successfully.
    /// Guarantees: The traces success counter is incremented.
    #[test]
    fn inc_exported_traces() {
        let mut m = new_metrics();
        m.inc_exported(SignalType::Traces);
        m.inc_exported(SignalType::Traces);
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Traces,
                    outcome: Outcome::Success
                })
                .messages
                .get(),
            2
        );
    }

    /// Scenario: Metrics are exported successfully.
    /// Guarantees: The metrics success counter is incremented.
    #[test]
    fn inc_exported_metrics() {
        let mut m = new_metrics();
        m.inc_exported(SignalType::Metrics);
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Metrics,
                    outcome: Outcome::Success
                })
                .messages
                .get(),
            1
        );
    }

    /// Scenario: Logs are exported successfully.
    /// Guarantees: The logs success counter is incremented.
    #[test]
    fn inc_exported_logs() {
        let mut m = new_metrics();
        m.inc_exported(SignalType::Logs);
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Logs,
                    outcome: Outcome::Success
                })
                .messages
                .get(),
            1
        );
    }

    /// Scenario: Traces export fails.
    /// Guarantees: The traces failure counter is incremented.
    #[test]
    fn inc_failed_traces() {
        let mut m = new_metrics();
        m.inc_failed(SignalType::Traces);
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Traces,
                    outcome: Outcome::Failure
                })
                .messages
                .get(),
            1
        );
    }

    /// Scenario: Metrics export fails.
    /// Guarantees: The metrics failure counter is incremented.
    #[test]
    fn inc_failed_metrics() {
        let mut m = new_metrics();
        m.inc_failed(SignalType::Metrics);
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Metrics,
                    outcome: Outcome::Failure
                })
                .messages
                .get(),
            1
        );
    }

    /// Scenario: Logs export fails.
    /// Guarantees: The logs failure counter is incremented.
    #[test]
    fn inc_failed_logs() {
        let mut m = new_metrics();
        m.inc_failed(SignalType::Logs);
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Logs,
                    outcome: Outcome::Failure
                })
                .messages
                .get(),
            1
        );
    }

    /// Scenario: Acks and nacks are received.
    /// Guarantees: Operational counters for acks and nacks are incremented correctly.
    #[test]
    fn inc_ack_and_nack() {
        let mut m = new_metrics();
        m.inc_ack();
        m.inc_ack();
        m.inc_nack();
        assert_eq!(m.operational_metrics.acks_received.get(), 2);
        assert_eq!(m.operational_metrics.nacks_received.get(), 1);
    }

    /// Scenario: Export, ACK, NACK, and topic routing events occur simultaneously.
    /// Guarantees: All counters increment independently without interfering with each other.
    #[test]
    fn counters_are_independent() {
        let mut m = new_metrics();
>>>>>>> main
        m.inc_exported(SignalType::Traces);
        m.inc_exported(SignalType::Metrics);
        m.inc_exported(SignalType::Logs);
        m.inc_failed(SignalType::Traces);
        m.inc_ack();
        m.inc_nack();
        m.inc_topic_from_header();
        m.inc_topic_from_static_config();

<<<<<<< HEAD
        assert_eq!(m.traces_exported.get(), 1);
        assert_eq!(m.metrics_exported.get(), 1);
        assert_eq!(m.logs_exported.get(), 1);
        assert_eq!(m.traces_failed.get(), 1);
        assert_eq!(m.metrics_failed.get(), 0);
        assert_eq!(m.logs_failed.get(), 0);
        assert_eq!(m.acks_received.get(), 1);
        assert_eq!(m.nacks_received.get(), 1);
        assert_eq!(m.topic_from_header.get(), 1);
        assert_eq!(m.topic_from_static_config.get(), 1);
    }

    #[test]
    fn inc_topic_from_header() {
        let mut m = KafkaExporterMetrics::default();
        m.inc_topic_from_header();
        m.inc_topic_from_header();
        assert_eq!(m.topic_from_header.get(), 2);
        assert_eq!(m.topic_from_static_config.get(), 0);
    }

    #[test]
    fn inc_topic_from_static_config() {
        let mut m = KafkaExporterMetrics::default();
        m.inc_topic_from_static_config();
        m.inc_topic_from_static_config();
        assert_eq!(m.topic_from_static_config.get(), 2);
        assert_eq!(m.topic_from_header.get(), 0);
=======
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Traces,
                    outcome: Outcome::Success
                })
                .messages
                .get(),
            1
        );
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Metrics,
                    outcome: Outcome::Success
                })
                .messages
                .get(),
            1
        );
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Logs,
                    outcome: Outcome::Success
                })
                .messages
                .get(),
            1
        );
        assert_eq!(
            m.export_metrics
                .with(SignalOutcomeAttributes {
                    signal: SignalType::Traces,
                    outcome: Outcome::Failure
                })
                .messages
                .get(),
            1
        );

        assert_eq!(m.operational_metrics.acks_received.get(), 1);
        assert_eq!(m.operational_metrics.nacks_received.get(), 1);
        assert_eq!(m.operational_metrics.topic_from_header.get(), 1);
        assert_eq!(m.operational_metrics.topic_from_static_config.get(), 1);
    }

    /// Scenario: Topic is resolved from a header.
    /// Guarantees: The corresponding operational counter is incremented.
    #[test]
    fn inc_topic_from_header() {
        let mut m = new_metrics();
        m.inc_topic_from_header();
        m.inc_topic_from_header();
        assert_eq!(m.operational_metrics.topic_from_header.get(), 2);
        assert_eq!(m.operational_metrics.topic_from_static_config.get(), 0);
    }

    /// Scenario: Topic is resolved from static config.
    /// Guarantees: The corresponding operational counter is incremented.
    #[test]
    fn inc_topic_from_static_config() {
        let mut m = new_metrics();
        m.inc_topic_from_static_config();
        m.inc_topic_from_static_config();
        assert_eq!(m.operational_metrics.topic_from_static_config.get(), 2);
        assert_eq!(m.operational_metrics.topic_from_header.get(), 0);
>>>>>>> main
    }
}
