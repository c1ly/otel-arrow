// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared functions and data types for contrib node implementations.

/// Shared Kafka utilities for Kafka receiver and exporter.
<<<<<<< HEAD
#[cfg(feature = "kafka-exporter")]
=======
#[cfg(any(feature = "kafka-receiver", feature = "kafka-exporter"))]
>>>>>>> main
pub mod kafka;
