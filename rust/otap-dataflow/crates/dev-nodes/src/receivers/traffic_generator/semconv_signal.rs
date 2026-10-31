// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//!
//! The semconv signal module provides methods for generating OTLP signals based on
//! the OpenTelemetry Semantic Conventions registry.
//!
//! Values are driven by `traffic-generator` annotations on the registry groups
//! and attributes. Annotations are read and sampled each time a value is
//! generated; groups or attributes without annotations fall back to enum
//! members, examples, and type defaults.
//!
//! ToDo: Add profile signal support -> update the builder lib.rs to work on profile object

use crate::receivers::traffic_generator::annotation::{
    self, DEFAULT_HISTOGRAM_BOUNDS, EventSpec, MetricSpec, SEVERITY_NAMES, STATUS_NAMES, SpanSpec,
};
use crate::receivers::traffic_generator::attributes::get_attribute_name_value;
use crate::receivers::traffic_generator::fake_data::{
    current_time, delay, gen_span_id, gen_trace_id, get_scope_name, get_scope_version,
};
use crate::receivers::traffic_generator::sampler::{sample_count, sample_f64, sample_string};
use otel_arrow_dfe_pdata::proto::opentelemetry::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs, SeverityNumber},
    metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData, span::Event, span::SpanKind},
};
use rand::Rng;
use weaver_forge::v1::registry::{ResolvedGroup, ResolvedRegistry};
use weaver_semconv::v1::group::{GroupType, InstrumentSpec, SpanKindSpec};

/// Value emitted for metrics without a `value` annotation.
const DEFAULT_METRIC_VALUE: f64 = 1.0;

fn resource() -> Resource {
    Resource::build()
        .attributes(vec![KeyValue::new(
            "traffic_generator",
            AnyValue::new_string("v1"),
        )])
        .finish()
}

fn scope() -> InstrumentationScope {
    InstrumentationScope::build()
        .name(get_scope_name())
        .version(get_scope_version())
        .finish()
}

/// Generates TracesData with the specified resource/scope count and defined spans in the registry
#[must_use]
pub fn semconv_otlp_traces(signal_count: usize, registry: &ResolvedRegistry) -> TracesData {
    semconv_otlp_traces_with_rng(signal_count, registry, &mut rand::rng())
}

/// Generates LogsData with the specified resource/scope count and defined events (structured logs) in the registry
#[must_use]
pub fn semconv_otlp_logs(signal_count: usize, registry: &ResolvedRegistry) -> LogsData {
    semconv_otlp_logs_with_rng(signal_count, registry, &mut rand::rng())
}

/// Generates MetricsData with the specified resource/scope count and defined metrics in the registry
#[must_use]
pub fn semconv_otlp_metrics(signal_count: usize, registry: &ResolvedRegistry) -> MetricsData {
    semconv_otlp_metrics_with_rng(signal_count, registry, &mut rand::rng())
}

/// Like [`semconv_otlp_traces`], drawing random values from `rng`.
pub fn semconv_otlp_traces_with_rng<R: Rng + ?Sized>(
    signal_count: usize,
    registry: &ResolvedRegistry,
    rng: &mut R,
) -> TracesData {
    TracesData::new(vec![ResourceSpans::new(
        resource(),
        vec![ScopeSpans::new(scope(), spans(signal_count, registry, rng))],
    )])
}

/// Like [`semconv_otlp_logs`], drawing random values from `rng`.
pub fn semconv_otlp_logs_with_rng<R: Rng + ?Sized>(
    signal_count: usize,
    registry: &ResolvedRegistry,
    rng: &mut R,
) -> LogsData {
    LogsData::new(vec![ResourceLogs::new(
        resource(),
        vec![ScopeLogs::new(scope(), logs(signal_count, registry, rng))],
    )])
}

/// Like [`semconv_otlp_metrics`], drawing random values from `rng`.
pub fn semconv_otlp_metrics_with_rng<R: Rng + ?Sized>(
    signal_count: usize,
    registry: &ResolvedRegistry,
    rng: &mut R,
) -> MetricsData {
    MetricsData::new(vec![ResourceMetrics::new(
        resource(),
        vec![ScopeMetrics::new(
            scope(),
            metrics(signal_count, registry, rng),
        )],
    )])
}

fn attributes<R: Rng + ?Sized>(group: &ResolvedGroup, rng: &mut R) -> Vec<KeyValue> {
    group
        .attributes
        .iter()
        .map(|a| get_attribute_name_value(a, rng))
        .collect()
}

// ---------------------------------------------------------------------------
// Signal-level annotation readers
// ---------------------------------------------------------------------------

fn span_spec(group: &ResolvedGroup) -> SpanSpec {
    annotation::lookup(group.annotations.as_ref())
        .ok()
        .flatten()
        .and_then(|m| annotation::parse_span_spec(m).ok())
        .unwrap_or_default()
}

fn event_spec(group: &ResolvedGroup) -> EventSpec {
    annotation::lookup(group.annotations.as_ref())
        .ok()
        .flatten()
        .and_then(|m| annotation::parse_event_spec(m).ok())
        .unwrap_or_default()
}

fn metric_spec(group: &ResolvedGroup, instrument: &InstrumentSpec) -> MetricSpec {
    annotation::lookup(group.annotations.as_ref())
        .ok()
        .flatten()
        .and_then(|m| annotation::parse_metric_spec(m, instrument).ok())
        .unwrap_or_default()
}

/// Span duration in nanoseconds (annotation `duration` is in milliseconds).
fn span_duration_ns<R: Rng + ?Sized>(spec: &SpanSpec, rng: &mut R) -> u64 {
    match &spec.duration {
        Some(d) => (sample_f64(d, false, rng) * 1_000_000.0).round().max(0.0) as u64,
        None => delay(),
    }
}

/// Span status from the `status` annotation, or `None` when absent.
fn span_status<R: Rng + ?Sized>(spec: &SpanSpec, rng: &mut R) -> Option<Status> {
    spec.status.as_ref().map(|s| {
        let code = status_code(&sample_string(s, rng)).unwrap_or(0);
        Status::new(code, "")
    })
}

/// Log severity number and text from the `severity` annotation (INFO when absent).
fn log_severity<R: Rng + ?Sized>(spec: &EventSpec, rng: &mut R) -> (i32, String) {
    match &spec.severity {
        Some(s) => {
            let name = sample_string(s, rng);
            let number = severity_number(&name).unwrap_or(SeverityNumber::Info as i32);
            (number, name)
        }
        None => (SeverityNumber::Info as i32, String::new()),
    }
}

/// Metric data point value from the `value` annotation (1.0 when absent).
fn metric_value<R: Rng + ?Sized>(spec: &MetricSpec, integer: bool, rng: &mut R) -> f64 {
    spec.value
        .as_ref()
        .map_or(DEFAULT_METRIC_VALUE, |v| sample_f64(v, integer, rng))
}

/// Number of data points per metric from the `data_points` annotation (1 when absent).
fn data_point_count<R: Rng + ?Sized>(spec: &MetricSpec, rng: &mut R) -> usize {
    spec.data_points
        .as_ref()
        .map_or(1, |d| sample_count(d, rng).max(1))
}

/// Map a severity short name to its SeverityNumber value (1..=24).
#[must_use]
pub fn severity_number(name: &str) -> Option<i32> {
    SEVERITY_NAMES
        .iter()
        .position(|n| *n == name)
        .map(|i| i as i32 + 1)
}

/// Map a status name to its StatusCode value.
#[must_use]
pub fn status_code(name: &str) -> Option<i32> {
    STATUS_NAMES
        .iter()
        .position(|n| *n == name)
        .map(|i| i as i32)
}

// ---------------------------------------------------------------------------
// Signals
// ---------------------------------------------------------------------------

/// generate each span defined in the resolved registry
fn spans<R: Rng + ?Sized>(
    signal_count: usize,
    registry: &ResolvedRegistry,
    rng: &mut R,
) -> Vec<Span> {
    let mut spans = vec![];
    for group in registry
        .groups
        .iter()
        .filter(|g| g.r#type == GroupType::Span)
        .cycle()
        .take(signal_count)
    {
        let spec = span_spec(group);
        let start_time = current_time();
        let end_time = start_time + span_duration_ns(&spec, rng);
        let builder = Span::build()
            .trace_id(gen_trace_id())
            .span_id(gen_span_id())
            .name(group.id.clone())
            .start_time_unix_nano(start_time)
            .attributes(attributes(group, rng))
            .events(
                group
                    .events
                    .iter()
                    .map(|event_name| {
                        Event::build()
                            .name(event_name.clone())
                            .time_unix_nano(current_time())
                            .finish()
                    })
                    .collect::<Vec<_>>(),
            )
            .kind(otel_span_kind(group.span_kind.as_ref()))
            .end_time_unix_nano(end_time);
        spans.push(match span_status(&spec, rng) {
            Some(status) => builder.status(status).finish(),
            None => builder.finish(),
        });
    }
    spans
}

/// generate each metric defined in the resolved registry
fn metrics<R: Rng + ?Sized>(
    signal_count: usize,
    registry: &ResolvedRegistry,
    rng: &mut R,
) -> Vec<Metric> {
    let mut metrics = vec![];
    for group in registry
        .groups
        .iter()
        .filter(|g| g.r#type == GroupType::Metric)
        .cycle()
        .take(signal_count)
    {
        let Some(instrument) = &group.instrument else {
            continue;
        };
        let spec = metric_spec(group, instrument);
        let int_value = annotation::metric_value_is_int(group.annotations.as_ref());
        let point_count = data_point_count(&spec, rng);
        let now = current_time();
        let builder = Metric::build()
            .name(group.metric_name.clone().unwrap_or_default())
            .description(group.brief.clone())
            .unit(group.unit.clone().unwrap_or_default());
        let metric = match instrument {
            InstrumentSpec::Histogram => {
                let points = (0..point_count)
                    .map(|_| histogram_point(group, &spec, now, rng))
                    .collect::<Vec<_>>();
                builder
                    .data_histogram(Histogram::new(AggregationTemporality::Delta, points))
                    .finish()
            }
            InstrumentSpec::Gauge => builder
                .data_gauge(Gauge::new(number_points(
                    group,
                    &spec,
                    int_value,
                    point_count,
                    now,
                    rng,
                )))
                .finish(),
            // Counters are per-interval increments with delta temporality, so
            // each sample is independent and replayed batches stay valid.
            InstrumentSpec::Counter => builder
                .data_sum(Sum::new(
                    AggregationTemporality::Delta,
                    true,
                    number_points(group, &spec, int_value, point_count, now, rng),
                ))
                .finish(),
            InstrumentSpec::UpDownCounter => builder
                .data_sum(Sum::new(
                    AggregationTemporality::Delta,
                    false,
                    number_points(group, &spec, int_value, point_count, now, rng),
                ))
                .finish(),
        };
        metrics.push(metric);
    }
    metrics
}

fn number_points<R: Rng + ?Sized>(
    group: &ResolvedGroup,
    spec: &MetricSpec,
    int_value: bool,
    count: usize,
    now: u64,
    rng: &mut R,
) -> Vec<NumberDataPoint> {
    (0..count)
        .map(|_| {
            let attrs = attributes(group, rng);
            let value = metric_value(spec, int_value, rng);
            let b = NumberDataPoint::build()
                .time_unix_nano(now)
                .attributes(attrs);
            if int_value {
                b.value_int(value.round() as i64).finish()
            } else {
                b.value_double(value).finish()
            }
        })
        .collect()
}

/// Aggregate sampled observations into explicit bucket counts.
///
/// Bucket `i` counts observations `x` with `bounds[i-1] < x <= bounds[i]`;
/// the last bucket counts `x > bounds[last]` (OTLP explicit bucket semantics).
#[must_use]
pub fn bucketize(observations: &[f64], bounds: &[f64]) -> Vec<u64> {
    let mut counts = vec![0u64; bounds.len() + 1];
    for x in observations {
        let idx = bounds.partition_point(|b| b < x);
        counts[idx] += 1;
    }
    counts
}

fn histogram_point<R: Rng + ?Sized>(
    group: &ResolvedGroup,
    spec: &MetricSpec,
    now: u64,
    rng: &mut R,
) -> HistogramDataPoint {
    let attrs = attributes(group, rng);
    let n = spec
        .observations
        .as_ref()
        .map_or(1, |o| sample_count(o, rng).max(1));
    // Histogram observations are aggregated as doubles.
    let observations = (0..n)
        .map(|_| metric_value(spec, false, rng))
        .collect::<Vec<_>>();
    let bounds = spec
        .bounds
        .clone()
        .unwrap_or_else(|| DEFAULT_HISTOGRAM_BOUNDS.to_vec());
    let sum = observations.iter().sum::<f64>();
    let min = observations.iter().copied().fold(f64::INFINITY, f64::min);
    let max = observations
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    HistogramDataPoint::build()
        .time_unix_nano(now)
        .count(observations.len() as u64)
        .sum(sum)
        .min(min)
        .max(max)
        .bucket_counts(bucketize(&observations, &bounds))
        .explicit_bounds(bounds)
        .attributes(attrs)
        .finish()
}

/// generate each event (structured log) defined in the resolved registry
fn logs<R: Rng + ?Sized>(
    signal_count: usize,
    registry: &ResolvedRegistry,
    rng: &mut R,
) -> Vec<LogRecord> {
    let mut log_records = vec![];
    for group in registry
        .groups
        .iter()
        .filter(|g| g.r#type == GroupType::Event)
        .cycle()
        .take(signal_count)
    {
        // events are structured logs
        let timestamp = current_time();
        let body_text = match &group.body {
            Some(body) => body.to_string(),
            None => String::new(),
        };
        let (number, text) = log_severity(&event_spec(group), rng);
        let builder = LogRecord::build()
            .time_unix_nano(timestamp)
            .severity_number(number)
            .event_name(group.name.clone().unwrap_or_default())
            .attributes(attributes(group, rng))
            .body(AnyValue::new_string(body_text))
            .observed_time_unix_nano(timestamp);
        log_records.push(if text.is_empty() {
            builder.finish()
        } else {
            builder.severity_text(text).finish()
        });
    }
    log_records
}

/// map a SpanKindSpec to a SpanKind
#[must_use]
const fn otel_span_kind(span_kind: Option<&SpanKindSpec>) -> SpanKind {
    match span_kind {
        Some(SpanKindSpec::Client) => SpanKind::Client,
        Some(SpanKindSpec::Server) => SpanKind::Server,
        Some(SpanKindSpec::Producer) => SpanKind::Producer,
        Some(SpanKindSpec::Consumer) => SpanKind::Consumer,
        Some(SpanKindSpec::Internal) | None => SpanKind::Internal,
    }
}
