// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! This module focuses on taking a filter definition for Metrics and building a filter
//! as a BooleanArray for the Metrics, ResourceAttr, MetricsAttr, MetricEvents, MetricEventAttrs, and MetricLinkAttrs OTAP Record Batches
//!

use crate::arrays::{get_required_array, get_required_array_from_struct_array_from_record_batch};
use crate::otap::OtapArrowRecords;
use crate::otap::error::{Error, Result};
use crate::otap::filter::{
    KeyValue, MatchType, NO_RECORD_BATCH_FILTER_SIZE, apply_filter, default_match_type,
    get_attr_filter, get_resource_attr_filter, new_child_record_batch_filter,
    new_parent_record_batch_filter, nulls_to_false, regex_match_column,
    update_child_record_batch_filter, update_parent_record_batch_filter,
};
use crate::otlp::metrics::MetricType;
use crate::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use crate::schema::consts;
use arrow::array::{BooleanArray, StringArray, UInt8Array};
use arrow::buffer::BooleanBuffer;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};

/// struct that describes the overall requirements to use in order to filter metrics
#[derive(Debug, Clone, Deserialize)]
pub struct MetricFilter {
    // Include match properties describe metrics that should be included in the Collector Service pipeline,
    // all other metrics should be dropped from further processing.
    // If both Include and Exclude are specified, Include filtering occurs first.
    include: Option<MetricMatchProperties>,
    // Exclude match properties describe metrics that should be excluded from the Collector Service pipeline,
    // all other metrics should be included.
    // If both Include and Exclude are specified, Include filtering occurs first.
    exclude: Option<MetricMatchProperties>,
    // ToDo: Add ottl support -> see golang version https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/8558294afa723b9ed917ed5d6bb6c656bb096a49/processor/filterprocessor/config.go#L90
}

/// MetricMatchProperties specifies the set of properties in a metric to match against and the type of string pattern matching to use.
#[derive(Debug, Clone, Deserialize)]
pub struct MetricMatchProperties {
    // MatchType specifies the type of matching desired
    #[serde(default = "default_match_type")]
    match_type: MatchType,

    // ResourceAttributes defines a list of possible resource attributes to match metrics against.
    // A match occurs if any resource attribute matches all expressions in this given list.
    #[serde(default)]
    resource_attributes: Vec<KeyValue>,

    // MetricAttributes defines a list of possible record attributes to match metrics against.
    // A match occurs if any record attribute matches at least one expression in this given list.
    #[serde(default)]
    datapoint_attributes: Vec<KeyValue>,

    // MetricNames is a list of metric names that the metric's names field must match against.
    #[serde(default)]
    metric_names: Vec<String>,

    // MetricTypes is a list of metrics types we want to filter from number, histogram, expoential histogram and summary,
    #[serde(default)]
    metric_types: HashSet<MetricType>,
}

impl MetricFilter {
    /// create a new metric filter
    #[must_use]
    pub fn new(
        include: Option<MetricMatchProperties>,
        exclude: Option<MetricMatchProperties>,
    ) -> Self {
        Self { include, exclude }
    }

    /// take a metrics payload and return the filtered result
    pub fn filter(
        &self,
        mut metrics_payload: OtapArrowRecords,
    ) -> Result<(OtapArrowRecords, u64, u64)> {
        let (
            resource_attr_filter,
            metric_filter,
            number_datapoint_attr_filter,
            summary_datapoint_attr_filter,
            histogram_datapoint_attr_filter,
            exp_histogram_datapoint_attr_filter,
        ) = if let Some(include_config) = &self.include
            && let Some(exclude_config) = &self.exclude
        {
            let (
                include_resource_attr_filter,
                include_metric_filter,
                include_number_datapoint_attr_filter,
                include_summary_datapoint_attr_filter,
                include_histogram_datapoint_attr_filter,
                include_exp_histogram_datapoint_attr_filter,
            ) = include_config.create_filters(&metrics_payload, false)?;
            let (
                exclude_resource_attr_filter,
                exclude_metric_filter,
                exclude_number_datapoint_attr_filter,
                exclude_summary_datapoint_attr_filter,
                exclude_histogram_datapoint_attr_filter,
                exclude_exp_histogram_datapoint_attr_filter,
            ) = exclude_config.create_filters(&metrics_payload, true)?;
            // combine the include and exclude filters
            let resource_attr_filter = arrow::compute::and_kleene(
                &include_resource_attr_filter,
                &exclude_resource_attr_filter,
            )
            .map_err(|e| Error::ColumnLengthMismatch { source: e })?;

            let metric_filter =
                arrow::compute::and_kleene(&include_metric_filter, &exclude_metric_filter)
                    .map_err(|e: arrow_schema::ArrowError| Error::ColumnLengthMismatch {
                        source: e,
                    })?;

            let number_datapoint_attr_filter = arrow::compute::and_kleene(
                &include_number_datapoint_attr_filter,
                &exclude_number_datapoint_attr_filter,
            )
            .map_err(|e: arrow_schema::ArrowError| Error::ColumnLengthMismatch { source: e })?;
            let summary_datapoint_attr_filter = arrow::compute::and_kleene(
                &include_summary_datapoint_attr_filter,
                &exclude_summary_datapoint_attr_filter,
            )
            .map_err(|e: arrow_schema::ArrowError| Error::ColumnLengthMismatch { source: e })?;
            let histogram_datapoint_attr_filter = arrow::compute::and_kleene(
                &include_histogram_datapoint_attr_filter,
                &exclude_histogram_datapoint_attr_filter,
            )
            .map_err(|e: arrow_schema::ArrowError| Error::ColumnLengthMismatch { source: e })?;
            let exp_histogram_datapoint_attr_filter = arrow::compute::and_kleene(
                &include_exp_histogram_datapoint_attr_filter,
                &exclude_exp_histogram_datapoint_attr_filter,
            )
            .map_err(|e: arrow_schema::ArrowError| Error::ColumnLengthMismatch { source: e })?;
            (
                resource_attr_filter,
                metric_filter,
                number_datapoint_attr_filter,
                summary_datapoint_attr_filter,
                histogram_datapoint_attr_filter,
                exp_histogram_datapoint_attr_filter,
            )
        } else if self.include.is_none()
            && let Some(exclude_config) = &self.exclude
        {
            exclude_config.create_filters(&metrics_payload, true)?
        } else if let Some(include_config) = &self.include
            && self.exclude.is_none()
        {
            include_config.create_filters(&metrics_payload, false)?
        } else {
            // both include and exclude is none
            let num_rows = metrics_payload
                .get(ArrowPayloadType::UnivariantMetrics)
                .ok_or_else(|| Error::RecordBatchNotFound {
                    payload_type: ArrowPayloadType::UnivariantMetrics,
                })?
                .num_rows() as u64;
            return Ok((metrics_payload, num_rows, num_rows));
        };

        let (metric_filter, child_record_batch_filters) = self.sync_up_filters(
            &metrics_payload,
            resource_attr_filter,
            metric_filter,
            number_datapoint_attr_filter,
            summary_datapoint_attr_filter,
            histogram_datapoint_attr_filter,
            exp_histogram_datapoint_attr_filter,
        )?;

        let (metric_rows_before, metric_rows_removed) = apply_filter(
            &mut metrics_payload,
            ArrowPayloadType::UnivariantMetrics,
            &metric_filter,
        )?;

        for (payload_type, filter) in child_record_batch_filters {
            let (_, _) = apply_filter(&mut metrics_payload, payload_type, &filter)?;
        }

        Ok((metrics_payload, metric_rows_before, metric_rows_removed))
    }

    /// this function takes the filters for each record batch and makes sure that incomplete
    /// returns the cleaned up filters that can be immediately applied on the record batches
    fn sync_up_filters(
        &self,
        metrics_payload: &OtapArrowRecords,
        resource_attr_filter: BooleanArray,
        mut metric_filter: BooleanArray,

        number_datapoint_attr_filter: BooleanArray,
        summary_datapoint_attr_filter: BooleanArray,
        histogram_datapoint_attr_filter: BooleanArray,
        exp_histogram_datapoint_attr_filter: BooleanArray,
    ) -> Result<(BooleanArray, HashMap<ArrowPayloadType, BooleanArray>)> {
        // get the record batches we are going to filter
        let resource_attrs = metrics_payload.get(ArrowPayloadType::ResourceAttrs);
        let metrics = metrics_payload
            .get(ArrowPayloadType::UnivariantMetrics)
            .ok_or_else(|| Error::MetricRecordNotFound {})?;
        let scope_attrs = metrics_payload.get(ArrowPayloadType::ScopeAttrs);
        let number_datapoints = metrics_payload.get(ArrowPayloadType::NumberDataPoints);
        let number_datapoint_attrs = metrics_payload.get(ArrowPayloadType::NumberDpAttrs);
        let number_datapoint_exemplars = metrics_payload.get(ArrowPayloadType::NumberDpExemplars);
        let number_datapoint_exemplar_attrs =
            metrics_payload.get(ArrowPayloadType::NumberDpExemplarAttrs);

        let summary_datapoints = metrics_payload.get(ArrowPayloadType::SummaryDataPoints);
        let summary_datapoint_attrs = metrics_payload.get(ArrowPayloadType::SummaryDpAttrs);

        let histogram_datapoints = metrics_payload.get(ArrowPayloadType::HistogramDataPoints);
        let histogram_datapoint_attrs = metrics_payload.get(ArrowPayloadType::HistogramDpAttrs);
        let histogram_datapoint_exemplars =
            metrics_payload.get(ArrowPayloadType::HistogramDpExemplars);
        let histogram_datapoint_exemplar_attrs =
            metrics_payload.get(ArrowPayloadType::HistogramDpExemplarAttrs);

        let exp_histogram_datapoints =
            metrics_payload.get(ArrowPayloadType::ExpHistogramDataPoints);
        let exp_histogram_datapoint_attrs =
            metrics_payload.get(ArrowPayloadType::ExpHistogramDpAttrs);
        let exp_histogram_datapoint_exemplars =
            metrics_payload.get(ArrowPayloadType::ExpHistogramDpExemplars);
        let exp_histogram_datapoint_exemplar_attrs =
            metrics_payload.get(ArrowPayloadType::ExpHistogramDpExemplarAttrs);

        // get the id columns from record batch
        let metric_ids_column = get_required_array(metrics, consts::ID)?;
        let metric_resource_ids_column = get_required_array_from_struct_array_from_record_batch(
            metrics,
            consts::RESOURCE,
            consts::ID,
        )?;
        let metric_scope_ids_column = get_required_array_from_struct_array_from_record_batch(
            metrics,
            consts::SCOPE,
            consts::ID,
        )?;

        // if we have number datapoint attributes then we can create the number datapoints filter
        let mut number_datapoint_filter = match number_datapoint_attrs {
            Some(number_datapoint_attrs_record_batch) => {
                // get event id column
                if let Some(number_datapoints_record_batch) = number_datapoints {
                    let number_datapoints_ids_column =
                        get_required_array(number_datapoints_record_batch, consts::ID)?;
                    Some(new_parent_record_batch_filter(
                        number_datapoint_attrs_record_batch,
                        number_datapoints_ids_column,
                        &number_datapoint_attr_filter,
                    )?)
                } else {
                    return Err(Error::UnexpectedRecordBatchState { reason: "Number Datapoint Attribute Record Batch found without Number Datapoint Record Batch".to_string() });
                }
            }
            None => {
                if number_datapoint_attr_filter.true_count() == 0 {
                    // the configuration required certain resource_attributes but found none so we can return early
                    // remove all elements as nothing matches
                    return Ok((
                        BooleanArray::from(BooleanBuffer::new_unset(metric_filter.len())),
                        HashMap::new(),
                    ));
                }
                None
            }
        };

        let mut summary_datapoint_filter = match summary_datapoint_attrs {
            Some(summary_datapoint_attrs_record_batch) => {
                if let Some(summary_datapoints_record_batch) = summary_datapoints {
                    let summary_datapoints_ids_column =
                        get_required_array(summary_datapoints_record_batch, consts::ID)?;
                    Some(new_parent_record_batch_filter(
                        summary_datapoint_attrs_record_batch,
                        summary_datapoints_ids_column,
                        &summary_datapoint_attr_filter,
                    )?)
                } else {
                    return Err(Error::UnexpectedRecordBatchState { reason: "Summary Datapoint Attribute Record Batch found without Summary Datapoint Record Batch".to_string() });
                }
            }
            None => {
                if summary_datapoint_attr_filter.true_count() == 0 {
                    return Ok((
                        BooleanArray::from(BooleanBuffer::new_unset(metric_filter.len())),
                        HashMap::new(),
                    ));
                }
                None
            }
        };

        let mut histogram_datapoint_filter = match histogram_datapoint_attrs {
            Some(histogram_datapoint_attrs_record_batch) => {
                if let Some(histogram_datapoints_record_batch) = histogram_datapoints {
                    let histogram_datapoints_ids_column =
                        get_required_array(histogram_datapoints_record_batch, consts::ID)?;
                    Some(new_parent_record_batch_filter(
                        histogram_datapoint_attrs_record_batch,
                        histogram_datapoints_ids_column,
                        &histogram_datapoint_attr_filter,
                    )?)
                } else {
                    return Err(Error::UnexpectedRecordBatchState { reason: "Histogram Datapoint Attribute Record Batch found without Histogram Datapoint Record Batch".to_string() });
                }
            }
            None => {
                if histogram_datapoint_attr_filter.true_count() == 0 {
                    return Ok((
                        BooleanArray::from(BooleanBuffer::new_unset(metric_filter.len())),
                        HashMap::new(),
                    ));
                }
                None
            }
        };

        let mut exp_histogram_datapoint_filter = match exp_histogram_datapoint_attrs {
            Some(exp_histogram_datapoint_attrs_record_batch) => {
                if let Some(exp_histogram_datapoints_record_batch) = exp_histogram_datapoints {
                    let exp_histogram_datapoints_ids_column =
                        get_required_array(exp_histogram_datapoints_record_batch, consts::ID)?;
                    Some(new_parent_record_batch_filter(
                        exp_histogram_datapoint_attrs_record_batch,
                        exp_histogram_datapoints_ids_column,
                        &exp_histogram_datapoint_attr_filter,
                    )?)
                } else {
                    return Err(Error::UnexpectedRecordBatchState { reason: "ExpHistogram Datapoint Attribute Record Batch found without ExpHistogram Datapoint Record Batch".to_string() });
                }
            }
            None => {
                if exp_histogram_datapoint_attr_filter.true_count() == 0 {
                    return Ok((
                        BooleanArray::from(BooleanBuffer::new_unset(metric_filter.len())),
                        HashMap::new(),
                    ));
                }
                None
            }
        };

        // optional record batch
        match resource_attrs {
            Some(resource_attrs_record_batch) => {
                metric_filter = update_parent_record_batch_filter(
                    resource_attrs_record_batch,
                    metric_resource_ids_column,
                    &resource_attr_filter,
                    &metric_filter,
                )?;
                // apply current logic
            }
            None => {
                if resource_attr_filter.true_count() == 0 {
                    // the configuration required certain resource_attributes but found none so we can return early
                    // remove all elements as nothing matches
                    return Ok((
                        BooleanArray::from(BooleanBuffer::new_unset(metric_filter.len())),
                        HashMap::new(),
                    ));
                }
            }
        }

        if let Some(datapoint_filter) = &number_datapoint_filter {
            match number_datapoints {
                Some(number_datapoints_record_batch) => {
                    metric_filter = update_parent_record_batch_filter(
                        number_datapoints_record_batch,
                        metric_ids_column,
                        datapoint_filter,
                        &metric_filter,
                    )?;
                }
                None => {
                    return Err(Error::UnexpectedRecordBatchState { reason: "Number Datapoint Filter created from Number Datapoint Attribute Record Batch but no Number Datapoint Record Batch found".to_string() });
                }
            }
        }

        if let Some(datapoint_filter) = &summary_datapoint_filter {
            match summary_datapoints {
                Some(summary_datapoints_record_batch) => {
                    metric_filter = update_parent_record_batch_filter(
                        summary_datapoints_record_batch,
                        metric_ids_column,
                        datapoint_filter,
                        &metric_filter,
                    )?;
                }
                None => {
                    return Err(Error::UnexpectedRecordBatchState { reason: "Summary Datapoint Filter created from Summary Datapoint Attribute Record Batch but no Summary Datapoint Record Batch found".to_string() });
                }
            }
        }

        if let Some(datapoint_filter) = &histogram_datapoint_filter {
            match histogram_datapoints {
                Some(histogram_datapoints_record_batch) => {
                    metric_filter = update_parent_record_batch_filter(
                        histogram_datapoints_record_batch,
                        metric_ids_column,
                        datapoint_filter,
                        &metric_filter,
                    )?;
                }
                None => {
                    return Err(Error::UnexpectedRecordBatchState { reason: "Histogram Datapoint Filter created from Histogram Datapoint Attribute Record Batch but no Histogram Datapoint Record Batch found".to_string() });
                }
            }
        }

        if let Some(datapoint_filter) = &exp_histogram_datapoint_filter {
            match exp_histogram_datapoints {
                Some(exp_histogram_datapoints_record_batch) => {
                    metric_filter = update_parent_record_batch_filter(
                        exp_histogram_datapoints_record_batch,
                        metric_ids_column,
                        datapoint_filter,
                        &metric_filter,
                    )?;
                }
                None => {
                    return Err(Error::UnexpectedRecordBatchState { reason: "ExpHistogram Datapoint Filter created from ExpHistogram Datapoint Attribute Record Batch but no ExpHistogram Datapoint Record Batch found".to_string() });
                }
            }
        }

        // now using the updated metric_filter we need to update the rest of the filers

        // use hashmap to map filters to their payload types to return,
        // only record batches that exist will have their filter added to this hashmap
        let mut child_record_batch_filters = HashMap::new();

        if let Some(resource_attrs_record_batch) = resource_attrs {
            _ = child_record_batch_filters.insert(
                ArrowPayloadType::ResourceAttrs,
                update_child_record_batch_filter(
                    resource_attrs_record_batch,
                    metric_resource_ids_column,
                    &resource_attr_filter,
                    &metric_filter,
                )?,
            );
        }

        if let Some(scope_attrs_record_batch) = scope_attrs {
            _ = child_record_batch_filters.insert(
                ArrowPayloadType::ScopeAttrs,
                new_child_record_batch_filter(
                    scope_attrs_record_batch,
                    metric_scope_ids_column,
                    &metric_filter,
                )?,
            );
        }

        Ok((metric_filter, child_record_batch_filters))
    }
}

impl MetricMatchProperties {
    #[must_use]
    pub fn new(
        match_type: MatchType,
        resource_attributes: Vec<KeyValue>,
        datapoint_attributes: Vec<KeyValue>,
        metric_names: Vec<String>,
        metric_types: HashSet<MetricType>,
    ) -> Self {
        Self {
            match_type,
            resource_attributes,
            datapoint_attributes,
            metric_names,
            metric_types,
        }
    }

    /// create filter takes a metrics_payload and returns the filters for each of the record batches, also takes a invert flag to determine if the filters will be inverted
    pub fn create_filters(
        &self,
        metrics_payload: &OtapArrowRecords,
        invert: bool,
    ) -> Result<(
        BooleanArray,
        BooleanArray,
        BooleanArray,
        BooleanArray,
        BooleanArray,
        BooleanArray,
    )> {
        let (
            mut resource_attr_filter,
            mut metric_filter,
            mut number_datapoint_attr_filter,
            mut summary_datapoint_attr_filter,
            mut histogram_datapoint_attr_filter,
            mut exp_histogram_datapoint_attr_filter,
        ) = (
            get_resource_attr_filter(metrics_payload, &self.resource_attributes, &self.match_type)?,
            self.get_metric_filter(metrics_payload)?,
            get_attr_filter(
                metrics_payload,
                &self.datapoint_attributes,
                &self.match_type,
                ArrowPayloadType::NumberDpAttrs,
            )?,
            get_attr_filter(
                metrics_payload,
                &self.datapoint_attributes,
                &self.match_type,
                ArrowPayloadType::SummaryDpAttrs,
            )?,
            get_attr_filter(
                metrics_payload,
                &self.datapoint_attributes,
                &self.match_type,
                ArrowPayloadType::HistogramDpAttrs,
            )?,
            get_attr_filter(
                metrics_payload,
                &self.datapoint_attributes,
                &self.match_type,
                ArrowPayloadType::ExpHistogramDpAttrs,
            )?,
        );

        // invert flag depending on whether we are excluding or including
        if invert {
            // default filter is all true

            // if no resource_attributes to filter on are defined then we can ignore them
            // that is we will resort to the default filter otherwise we can invert if the flag is set
            if !self.resource_attributes.is_empty() {
                resource_attr_filter =
                    arrow::compute::not(&resource_attr_filter).expect("not doesn't fail");
            }

            // if no metric_names and metric types to filter on are defined then we can ignore them
            // that is we will resort to the default filter otherwise we can invert if the flag is set
            if !self.metric_names.is_empty() && !self.metric_types.is_empty() {
                metric_filter = arrow::compute::not(&metric_filter).expect("not doesn't fail");
            }

            // if no datapoint_attributes to filter on are defined then we can ignore them
            // that is we will resort to the default filter otherwise we can invert if the flag is set
            if !self.datapoint_attributes.is_empty() {
                number_datapoint_attr_filter =
                    arrow::compute::not(&number_datapoint_attr_filter).expect("not doesn't fail");
            }

            // if no datapoint_attributes to filter on are defined then we can ignore them
            // that is we will resort to the default filter otherwise we can invert if the flag is set
            if !self.datapoint_attributes.is_empty() {
                summary_datapoint_attr_filter =
                    arrow::compute::not(&summary_datapoint_attr_filter).expect("not doesn't fail");
            }

            // if no datapoint_attributes to filter on are defined then we can ignore them
            // that is we will resort to the default filter otherwise we can invert if the flag is set
            if !self.datapoint_attributes.is_empty() {
                histogram_datapoint_attr_filter =
                    arrow::compute::not(&histogram_datapoint_attr_filter)
                        .expect("not doesn't fail");
            }

            // if no datapoint_attributes to filter on are defined then we can ignore them
            // that is we will resort to the default filter otherwise we can invert if the flag is set
            if !self.datapoint_attributes.is_empty() {
                exp_histogram_datapoint_attr_filter =
                    arrow::compute::not(&exp_histogram_datapoint_attr_filter)
                        .expect("not doesn't fail");
            }
        }

        Ok((
            resource_attr_filter,
            metric_filter,
            number_datapoint_attr_filter,
            summary_datapoint_attr_filter,
            histogram_datapoint_attr_filter,
            exp_histogram_datapoint_attr_filter,
        ))
    }

    /// Creates a booleanarray that will filter a metric record batch based on the
    /// metric name. A metric should have one of the defined metric names
    fn get_metric_filter(&self, metrics_payload: &OtapArrowRecords) -> Result<BooleanArray> {
        let metrics = metrics_payload
            .get(ArrowPayloadType::UnivariantMetrics)
            .ok_or_else(|| Error::MetricRecordNotFound)?;
        let num_rows = metrics.num_rows();

        let mut filter: BooleanArray = BooleanArray::from(BooleanBuffer::new_set(num_rows));
        // filter on metric names
        if !&self.metric_names.is_empty() {
            // create filter for metric names
            let names_column = get_required_array(metrics, consts::NAME)?;
            let mut metric_names_filter = BooleanArray::new_null(num_rows);
            for name in &self.metric_names {
                // match on body value
                let metric_name_filter = match self.match_type {
                    MatchType::Regexp => regex_match_column(names_column, name)?,
                    MatchType::Strict => {
                        let value_scalar = StringArray::new_scalar(name);
                        // since we use a scalar here we don't have to worry a column length mismatch when we compare

                        arrow::compute::kernels::cmp::eq(&names_column, &value_scalar)
                            .expect("can compare string value column to string scalar")
                    }
                };
                metric_names_filter =
                    arrow::compute::or_kleene(&metric_name_filter, &metric_names_filter)
                        .map_err(|e| Error::ColumnLengthMismatch { source: e })?;
                // combine the filters
            }
            filter = arrow::compute::and_kleene(&filter, &metric_names_filter)
                .map_err(|e| Error::ColumnLengthMismatch { source: e })?;
        }

        // filter on metric types
        if !self.metric_types.is_empty() {
            let metric_types_column = get_required_array(metrics, consts::METRIC_TYPE)?;
            let mut metric_types_filter = BooleanArray::new_null(num_rows);
            for metric_type in self.metric_types.iter() {
                let metric_type_scalar = UInt8Array::new_scalar(*metric_type as u8);
                let metric_type_filter =
                    arrow::compute::kernels::cmp::eq(&metric_types_column, &metric_type_scalar)
                        .expect("can compare u8int value column to uint8 scalar");
                metric_types_filter =
                    arrow::compute::or_kleene(&metric_type_filter, &metric_types_filter)
                        .map_err(|e| Error::ColumnLengthMismatch { source: e })?;
            }

            filter = arrow::compute::and_kleene(&filter, &metric_types_filter)
                .map_err(|e| Error::ColumnLengthMismatch { source: e })?;
        }

        Ok(nulls_to_false(&filter))
    }
}
