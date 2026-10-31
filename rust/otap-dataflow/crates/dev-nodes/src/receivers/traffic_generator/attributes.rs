// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Handles mapping attributes defined in a resolved registry to a KeyValue pair for a otlp signal
//!
//! Attribute values are chosen in this order:
//!
//! 1. The `traffic-generator` annotation on the resolved attribute (which, for
//!    Weaver v1 references, is either the reference's or the definition's
//!    annotation).
//! 2. Enum members, sampled uniformly.
//! 3. `examples`, sampled uniformly.
//! 4. A type default.

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue};
use rand::Rng;
use rand::seq::IndexedRandom;
use weaver_resolved_schema::v1::attribute::Attribute;
use weaver_semconv::v1::attribute::ValueSpec;
use weaver_semconv::v1::attribute::{
    AttributeType, Examples, PrimitiveOrArrayTypeSpec, TemplateTypeSpec,
};

use super::annotation::{self, AttributeSpec, Scalar};
use super::sampler::{sample_count, sample_string, sample_value};

fn scalar_to_any(value: Scalar) -> AnyValue {
    match value {
        Scalar::Int(i) => AnyValue::new_int(i),
        Scalar::Double(d) => AnyValue::new_double(d),
        Scalar::String(s) => AnyValue::new_string(s),
        Scalar::Bool(b) => AnyValue::new_bool(b),
    }
}

fn semconv_to_any(value: &ValueSpec) -> AnyValue {
    match value {
        ValueSpec::Int(i) => AnyValue::new_int(*i),
        ValueSpec::Double(d) => AnyValue::new_double(f64::from(*d)),
        ValueSpec::String(s) => AnyValue::new_string(s.clone()),
        ValueSpec::Bool(b) => AnyValue::new_bool(*b),
    }
}

/// Whether continuous samples for this attribute type should be integers.
const fn is_int_type(ty: &AttributeType) -> bool {
    matches!(
        ty,
        AttributeType::PrimitiveOrArray(
            PrimitiveOrArrayTypeSpec::Int | PrimitiveOrArrayTypeSpec::Ints
        ) | AttributeType::Template(TemplateTypeSpec::Int | TemplateTypeSpec::Ints)
    )
}

/// Sample the value part of an annotation spec.
fn sample_spec<R: Rng + ?Sized>(spec: &AttributeSpec, integer: bool, rng: &mut R) -> AnyValue {
    match spec {
        AttributeSpec::Scalar(v) => scalar_to_any(sample_value(v, integer, rng)),
        AttributeSpec::Array { length, items } => {
            let n = sample_count(length, rng);
            AnyValue::new_array(
                (0..n)
                    .map(|_| scalar_to_any(sample_value(items, integer, rng)))
                    .collect::<Vec<_>>(),
            )
        }
        AttributeSpec::Template { value, .. } => sample_spec(value, integer, rng),
    }
}

/// For the given attribute, return a name/value pair.
///
/// The value comes from the attribute's `traffic-generator` annotation when
/// present, otherwise from enum members, then examples, then a type default.
/// Annotations are validated when the registry is loaded; if one fails to
/// parse here the attribute falls back to the non-annotated behavior.
pub fn get_attribute_name_value<R: Rng + ?Sized>(attribute: &Attribute, rng: &mut R) -> KeyValue {
    if let Ok(Some(map)) = annotation::lookup(attribute.annotations.as_ref())
        && let Ok(spec) = annotation::parse_attribute_spec(map, &attribute.r#type)
    {
        let integer = is_int_type(&attribute.r#type);
        let value = sample_spec(&spec, integer, rng);
        let name = match &spec {
            AttributeSpec::Template { keys, .. } => {
                format!("{}.{}", attribute.name, sample_string(keys, rng))
            }
            _ => attribute.name.clone(),
        };
        return KeyValue::new(name, value);
    }
    fallback_name_value(attribute, rng)
}

/// Enum / example / default based value generation used when no annotation is present.
fn fallback_name_value<R: Rng + ?Sized>(attribute: &Attribute, rng: &mut R) -> KeyValue {
    let name = attribute.name.clone();
    match &attribute.r#type {
        AttributeType::PrimitiveOrArray(primitive_or_array) => {
            let value = match primitive_or_array {
                PrimitiveOrArrayTypeSpec::Boolean => AnyValue::new_bool(true),
                PrimitiveOrArrayTypeSpec::Int => match &attribute.examples {
                    Some(Examples::Int(i)) => AnyValue::new_int(*i),
                    Some(Examples::Ints(ints)) => {
                        AnyValue::new_int(*ints.choose(rng).unwrap_or(&42))
                    }
                    _ => AnyValue::new_int(42),
                },
                PrimitiveOrArrayTypeSpec::Double => match &attribute.examples {
                    Some(Examples::Double(d)) => AnyValue::new_double(f64::from(*d)),
                    Some(Examples::Doubles(doubles)) => AnyValue::new_double(f64::from(
                        *doubles.choose(rng).unwrap_or(&(3.13).into()),
                    )),
                    _ => AnyValue::new_double(3.13),
                },
                PrimitiveOrArrayTypeSpec::String => match &attribute.examples {
                    Some(Examples::String(s)) => AnyValue::new_string(s.to_string()),
                    Some(Examples::Strings(strings)) => AnyValue::new_string(
                        strings
                            .choose(rng)
                            .unwrap_or(&"value".to_owned())
                            .clone()
                            .to_string(),
                    ),
                    _ => AnyValue::new_string("value".to_string()),
                },
                PrimitiveOrArrayTypeSpec::Any => match &attribute.examples {
                    // Boolean-based examples
                    Some(Examples::Bool(b)) => AnyValue::new_bool(*b),
                    Some(Examples::Bools(booleans)) => {
                        AnyValue::new_bool(*booleans.choose(rng).unwrap_or(&true))
                    }
                    Some(Examples::ListOfBools(list_of_bools)) => AnyValue::new_array(
                        list_of_bools
                            .choose(rng)
                            .unwrap_or(&vec![true, false])
                            .iter()
                            .map(|b| AnyValue::new_bool(*b))
                            .collect::<Vec<_>>(),
                    ),

                    // Integer-based examples
                    Some(Examples::Int(i)) => AnyValue::new_int(*i),
                    Some(Examples::Ints(ints)) => {
                        AnyValue::new_int(*ints.choose(rng).unwrap_or(&42))
                    }
                    Some(Examples::ListOfInts(list_of_ints)) => AnyValue::new_array(
                        list_of_ints
                            .choose(rng)
                            .unwrap_or(&vec![42, 43])
                            .iter()
                            .map(|i| AnyValue::new_int(*i))
                            .collect::<Vec<_>>(),
                    ),
                    // Double-based examples
                    Some(Examples::Double(d)) => AnyValue::new_double(f64::from(*d)),
                    Some(Examples::Doubles(doubles)) => AnyValue::new_double(f64::from(
                        *doubles.choose(rng).unwrap_or(&(3.13).into()),
                    )),
                    Some(Examples::ListOfDoubles(list_of_doubles)) => AnyValue::new_array(
                        list_of_doubles
                            .choose(rng)
                            .unwrap_or(&vec![(3.13).into(), (3.15).into()])
                            .iter()
                            .map(|d| AnyValue::new_double(f64::from(*d)))
                            .collect::<Vec<_>>(),
                    ),
                    // String-based examples
                    Some(Examples::String(s)) => AnyValue::new_string(s.clone()),
                    Some(Examples::Strings(strings)) => AnyValue::new_string(
                        strings
                            .choose(rng)
                            .unwrap_or(&"value".to_owned())
                            .clone()
                            .to_string(),
                    ),
                    Some(Examples::ListOfStrings(list_of_strings)) => AnyValue::new_array(
                        list_of_strings
                            .choose(rng)
                            .unwrap_or(&vec!["value1".to_string(), "value2".to_string()])
                            .iter()
                            .map(AnyValue::new_string)
                            .collect::<Vec<_>>(),
                    ),
                    Some(Examples::Any(any)) => match any {
                        ValueSpec::Int(v) => AnyValue::new_int(*v),
                        ValueSpec::Double(v) => AnyValue::new_double(f64::from(*v)),
                        ValueSpec::String(v) => AnyValue::new_string(v.clone()),
                        ValueSpec::Bool(v) => AnyValue::new_bool(*v),
                    },
                    Some(Examples::Anys(anys)) => anys
                        .choose(rng)
                        .map(|v| match v {
                            ValueSpec::Int(v) => AnyValue::new_int(*v),
                            ValueSpec::Double(v) => AnyValue::new_double(f64::from(*v)),
                            ValueSpec::String(v) => AnyValue::new_string(v.clone()),
                            ValueSpec::Bool(v) => AnyValue::new_bool(*v),
                        })
                        .unwrap_or(AnyValue::new_string("value".to_string())),
                    // Fallback to a default value
                    _ => AnyValue::new_string("value".to_string()),
                },
                PrimitiveOrArrayTypeSpec::Booleans => {
                    AnyValue::new_array(vec![AnyValue::new_bool(true), AnyValue::new_bool(false)])
                }
                PrimitiveOrArrayTypeSpec::Ints => match &attribute.examples {
                    Some(Examples::Ints(ints)) => AnyValue::new_array(
                        ints.iter()
                            .map(|i64| AnyValue::new_int(*i64))
                            .collect::<Vec<_>>(),
                    ),
                    Some(Examples::ListOfInts(list_of_ints)) => AnyValue::new_array(
                        list_of_ints
                            .choose(rng)
                            .unwrap_or(&vec![42, 43])
                            .iter()
                            .map(|i| AnyValue::new_int(*i))
                            .collect::<Vec<_>>(),
                    ),
                    _ => AnyValue::new_array(vec![AnyValue::new_int(42), AnyValue::new_int(43)]),
                },
                PrimitiveOrArrayTypeSpec::Doubles => match &attribute.examples {
                    Some(Examples::Doubles(doubles)) => AnyValue::new_array(
                        doubles
                            .iter()
                            .map(|d| AnyValue::new_double(f64::from(*d)))
                            .collect::<Vec<_>>(),
                    ),
                    Some(Examples::ListOfDoubles(list_of_doubles)) => AnyValue::new_array(
                        list_of_doubles
                            .choose(rng)
                            .unwrap_or(&vec![(3.13).into(), (3.15).into()])
                            .iter()
                            .map(|d| AnyValue::new_double(f64::from(*d)))
                            .collect::<Vec<_>>(),
                    ),
                    _ => AnyValue::new_array(vec![
                        AnyValue::new_double(3.13),
                        AnyValue::new_double(3.15),
                    ]),
                },
                PrimitiveOrArrayTypeSpec::Strings => match &attribute.examples {
                    Some(Examples::Strings(strings)) => AnyValue::new_array(
                        strings.iter().map(AnyValue::new_string).collect::<Vec<_>>(),
                    ),
                    Some(Examples::ListOfStrings(list_of_strings)) => AnyValue::new_array(
                        list_of_strings
                            .choose(rng)
                            .unwrap_or(&vec!["value1".to_string(), "value2".to_string()])
                            .iter()
                            .map(AnyValue::new_string)
                            .collect::<Vec<_>>(),
                    ),
                    _ => AnyValue::new_array(vec![
                        AnyValue::new_string("value1".to_string()),
                        AnyValue::new_string("value2".to_string()),
                    ]),
                },
            };
            KeyValue::new(name, value)
        }
        AttributeType::Enum { members, .. } => {
            let value = members
                .choose(rng)
                .map(|m| semconv_to_any(&m.value))
                .unwrap_or_else(|| AnyValue::new_string("value".to_string()));
            KeyValue::new(name, value)
        }
        AttributeType::Template(template_type_spec) => {
            // TODO Support examples when https://github.com/open-telemetry/semantic-conventions/issues/1740 is complete
            let value = match template_type_spec {
                TemplateTypeSpec::String => AnyValue::new_string("template_value".to_string()),
                TemplateTypeSpec::Int => AnyValue::new_int(42),
                TemplateTypeSpec::Double => AnyValue::new_double(3.13),
                TemplateTypeSpec::Boolean => AnyValue::new_bool(true),
                TemplateTypeSpec::Any => AnyValue::new_string("template_any_value".to_string()),
                TemplateTypeSpec::Strings => AnyValue::new_array(vec![
                    AnyValue::new_string("template_value1".to_string()),
                    AnyValue::new_string("template_value2".to_string()),
                ]),
                TemplateTypeSpec::Ints => {
                    AnyValue::new_array(vec![AnyValue::new_int(42), AnyValue::new_int(43)])
                }
                TemplateTypeSpec::Doubles => AnyValue::new_array(vec![
                    AnyValue::new_double(3.13),
                    AnyValue::new_double(3.15),
                ]),
                TemplateTypeSpec::Booleans => {
                    AnyValue::new_array(vec![AnyValue::new_bool(true), AnyValue::new_bool(false)])
                }
            };
            KeyValue::new(name, value)
        }
    }
}
