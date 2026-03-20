// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Graceful degradation validation.
//!
//! Validates that under fault conditions the pipeline still delivers signals
//! within acceptable latency and delivery-ratio bounds.

use otap_df_pdata::proto::OtlpProtoMessage;
use std::time::Duration;

/// Validate graceful degradation under faults.
///
/// - Checks that at least `min_delivery_ratio` of control signals were
///   delivered by the system-under-validation.
/// - Checks that every delivered SUV message arrived within `max_latency`.
pub(crate) fn validate_graceful_degradation(
    control: &[OtlpProtoMessage],
    suv: &[(OtlpProtoMessage, Duration)],
    max_latency: Duration,
    min_delivery_ratio: f64,
) -> bool {
    let control_total: usize = control.iter().map(OtlpProtoMessage::num_items).sum();
    let suv_total: usize = suv.iter().map(|(msg, _)| msg.num_items()).sum();

    // Check delivery ratio
    if control_total > 0 {
        let delivery_ratio = suv_total as f64 / control_total as f64;
        if delivery_ratio < min_delivery_ratio {
            return false;
        }
    }

    // Check latency bounds for all SUV messages
    for (_, latency) in suv {
        if *latency > max_latency {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };

    fn logs_with_records(count: usize) -> OtlpProtoMessage {
        OtlpProtoMessage::Logs(LogsData {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord::default(); count],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        })
    }

    #[test]
    fn passes_when_within_bounds() {
        let control = vec![logs_with_records(10)];
        let suv = vec![
            (logs_with_records(5), Duration::from_millis(100)),
            (logs_with_records(4), Duration::from_millis(200)),
        ];
        assert!(validate_graceful_degradation(
            &control,
            &suv,
            Duration::from_secs(1),
            0.8
        ));
    }

    #[test]
    fn fails_when_latency_exceeded() {
        let control = vec![logs_with_records(10)];
        let suv = vec![(logs_with_records(10), Duration::from_secs(5))];
        assert!(!validate_graceful_degradation(
            &control,
            &suv,
            Duration::from_secs(1),
            0.9
        ));
    }

    #[test]
    fn fails_when_delivery_ratio_too_low() {
        let control = vec![logs_with_records(10)];
        let suv = vec![(logs_with_records(5), Duration::from_millis(100))];
        assert!(!validate_graceful_degradation(
            &control,
            &suv,
            Duration::from_secs(1),
            0.9
        ));
    }

    #[test]
    fn passes_with_full_delivery() {
        let control = vec![logs_with_records(10)];
        let suv = vec![(logs_with_records(10), Duration::from_millis(50))];
        assert!(validate_graceful_degradation(
            &control,
            &suv,
            Duration::from_secs(1),
            1.0
        ));
    }

    #[test]
    fn empty_control_passes() {
        let suv = vec![(logs_with_records(5), Duration::from_millis(50))];
        assert!(validate_graceful_degradation(
            &[],
            &suv,
            Duration::from_secs(1),
            1.0
        ));
    }

    #[test]
    fn empty_suv_fails_when_ratio_required() {
        let control = vec![logs_with_records(10)];
        assert!(!validate_graceful_degradation(
            &control,
            &[],
            Duration::from_secs(1),
            0.5
        ));
    }
}
