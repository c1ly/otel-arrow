# Traffic Generator Semantic Convention Annotations

Status: implemented in the `traffic_generator` receiver. The annotated
registry is the vendored copy in `vendor/semconv/model`; review items are
tracked in [temp-annotation-revisit.md](temp-annotation-revisit.md).

## Overview

When the traffic generator receiver runs with
`data_source: semantic_conventions`, it builds spans, metrics, and logs from a
resolved semantic convention registry. Today the generated values carry very
little variety: attribute values come from `examples` or hardcoded constants,
metric data points are always `1.0`, and span durations are fixed.

This document specifies an annotation-driven value generation model. Registry
authors attach a `traffic-generator` annotation to attributes and signals to
describe the set of acceptable values (a list or a range) and the distribution
used to sample them. The receiver reads these annotations when it loads the
registry and uses them to generate data.

The annotations live in the vendored semantic conventions registry
(`rust/otap-dataflow/vendor/semconv/model`, upstream `v1.44.0`), annotated in
place. Tests load it through `config::vendored_registry_path()`. The receiver's
default `registry_path` still points at the upstream git repository, which has
no `traffic-generator` annotations; point `registry_path` at the vendored
directory to use them. Without annotations the receiver falls back to
examples, enum members, and type defaults.

## Background

### Registry loading

`Config::get_registry()` (`config.rs`) loads the registry pointed to by
`registry_path` using Weaver (`weaver_resolver`, `weaver_forge`), resolves it
with `include_unreferenced: true`, and converts it into a v1
`weaver_forge::v1::registry::ResolvedRegistry`. The generator then iterates over
the resolved groups by `GroupType` (`span`, `metric`, `event`).

### Where annotations are available

Weaver carries annotations through resolution without interpreting them. The
value type is `YamlValue` (a transparent wrapper around
`serde_yaml::Value`).

| Location | Field |
| --- | --- |
| Resolved attribute | `Attribute.annotations: Option<BTreeMap<String, YamlValue>>` |
| Enum member | `EnumEntriesSpec.annotations: Option<BTreeMap<String, YamlValue>>` |
| Resolved group (span / metric / event) | `ResolvedGroup.annotations: Option<BTreeMap<String, YamlValue>>` |

When a signal references an attribute (`ref:`) and sets `annotations` on the
reference, Weaver v1 replaces the definition's entire annotation map with the
reference's map (no deep merge). The annotations a signal sees on its resolved
attribute are therefore either the reference's annotations or, if the
reference has none, the definition's annotations. Registry authors must repeat
any other annotation namespaces on the reference if they need them.

### Current value generation

`attributes.rs::get_attribute_name_value` and `semconv_signal.rs` produce
values as follows:

| Target | Current behavior |
| --- | --- |
| `int`, `double`, `string` | Uniform random pick from `examples`; constant (`42`, `3.13`, `"value"`) when absent |
| `boolean` | Always `true` |
| `string[]`, `int[]`, `double[]` | Whole example list as the array, or a random list from list-of-lists examples; constants otherwise |
| `boolean[]` | Always `[true, false]` |
| `any` | Uniform pick across any example shape |
| enum | Always `members[0]` (no variety; panics on empty members) |
| `template[T]` | Constants; the attribute name is the bare template prefix |
| Metric data points | One point, value `1.0`, `AggregationTemporality::Unspecified` |
| Histogram data points | Empty (no buckets, count, or sum) |
| Span duration | Constant 10 microseconds |
| Span status | Not set |
| Log severity | Always `INFO` |

Randomness uses the unseeded thread RNG (`rand::rng()`), and the only sampling
is uniform selection.

`examples` are documentation, not a definition of the valid value space. They
typically contain one to three values and cannot express ranges, weights, or
distributions. Annotations become the primary source of values; `examples`
remain a fallback (see [Value precedence](#value-precedence)).

### Existing annotation namespaces

The upstream semantic convention registry already uses annotation namespaces
owned by other tools, for example:

| Namespace | Consumer | Example keys |
| --- | --- | --- |
| `code_generation` | Weaver JQ helpers, language code generators | `exclude`, `metric_value_type` |
| `dependency_resolution` | Weaver resolver | `exclude` |
| `markdown` | Weaver documentation templates | `omit_enum_table` |
| `naming_conventions`, `compatibility` | Semantic convention policies | `policy_exceptions` |

## Annotation namespace

All traffic generator annotations live under the `traffic-generator` key:

```yaml
annotations:
  traffic-generator:
    values: [200, 404, 500]
    weights: [90, 7, 3]
```

The namespace is read only by the traffic generator receiver. Weaver and other
tools pass it through untouched.

The key contains a hyphen, so template and policy languages must use bracket
access rather than dotted access:

| Language | Access |
| --- | --- |
| JQ | `.annotations["traffic-generator"]` |
| Jinja (Weaver templates) | `attribute.annotations["traffic-generator"]` |
| Rego | `attr.annotations["traffic-generator"]` |
| Rust | `annotations.get("traffic-generator")` |

## Parsing

The annotation carries no version or type tag. The receiver interprets it from
two things it already has: the context (the attribute type or signal group type
from the resolved registry) and which keys are present in the
`traffic-generator` map.

### Key detection

| Context | Keys checked | Interpretation |
| --- | --- | --- |
| Scalar attribute (`int`, `double`, `string`, `boolean`, enum, `any`) | `values` present | Discrete form: read `values`, then `weights` if present |
| | `range` present | Continuous form: read `range`, then `distribution` if present |
| | both present | Error: `values` and `range` are mutually exclusive |
| | neither present | Error: a value spec requires `values` or `range` |
| Array attribute (`T[]`) | `length`, `items` | Each is a nested value spec, parsed with the same `values` / `range` detection |
| Template attribute (`template[T]`) | `keys`, `value` | Each is a nested spec; for array templates `value` contains `length` / `items` |
| Metric group | `value`, `data_points`, `observations`, `bounds` | Each key present is parsed as a value spec (`bounds` is a list of numbers) |
| Span group | `duration`, `status` | Each key present is parsed as a value spec |
| Event group | `severity` | Parsed as a value spec |
| No `traffic-generator` annotation | none | Fall back per [Value precedence](#value-precedence) |

Pairing rules checked during the same lookup:

- `weights` without `values` is an error.
- `distribution` without `range` is an error.

### Unknown keys

Every map in the annotation has a fixed set of allowed keys for its context.
Any other key is rejected at registry load time. This catches typos (for
example `weight` instead of `weights`) that would otherwise silently fall back
to uniform sampling.

| Map | Allowed keys |
| --- | --- |
| Scalar attribute spec | `values`, `weights`, `range`, `distribution` |
| Array attribute spec | `length`, `items` |
| Template attribute spec | `keys`, `value` |
| Nested value spec (`length`, `items`, `keys`, scalar `value`, signal keys) | `values`, `weights`, `range`, `distribution` |
| `range` | `min`, `max` |
| `distribution` | `type` plus the parameters of that type (see [Distributions](#distributions)) |
| Metric group (counter, updowncounter, gauge) | `value`, `data_points` |
| Metric group (histogram) | `value`, `data_points`, `observations`, `bounds` |
| Span group | `duration`, `status` |
| Event group | `severity` |

Distribution parameters are checked per type: `exponent` is rejected on
`normal`, `stddev` is rejected on `exponential`, and so on.

Errors include the full key path:

```text
http.response.status_code: traffic-generator: unknown key "weight"
example.int.normal: traffic-generator.distribution: unknown key "sd" for type normal
span.example.client: traffic-generator: unknown key "observations" for span group
```

## Value spec

Every generated value is described by a single structure, the value spec. It
has exactly two forms.

| Form | Keys | Meaning |
| --- | --- | --- |
| Discrete | `values` (required), `weights` (optional) | Choose one entry from `values`. Uniform when `weights` is omitted. |
| Continuous | `range` (required), `distribution` (optional) | Sample a number in `[min, max]`. Uniform when `distribution` is omitted. |

```yaml
# Discrete form
values: [<v1>, <v2>, ...]
weights: [<w1>, <w2>, ...]       # optional

# Continuous form (int and double only)
range: { min: <number>, max: <number> }
distribution:                    # optional, default uniform
  type: uniform | normal | lognormal | exponential | zipf
  # parameters depend on type (see below)
```

Rules:

- `values` and `range` are mutually exclusive; exactly one must be present.
- `weights` is only valid with `values`.
- `distribution` is only valid with `range`.
- `weights` and `distribution` are therefore mutually exclusive.
- `weights` has the same length as `values`, every weight is a finite number
  `>= 0`, and the sum is `> 0`. Weights are relative and need not sum to 100.
- `range.min <= range.max`; both bounds are inclusive.

### Distributions

All parameters are required and are expressed in the units of the generated
value. They describe the distribution before truncation to `range`, so the
mean of the generated values differs from `mean` when the range cuts off part
of the distribution.

| `type` | Parameters | Constraints | Applies to |
| --- | --- | --- | --- |
| `uniform` | none | none | int, double |
| `normal` | `mean`, `stddev` | `stddev > 0` | int, double |
| `lognormal` | `mean`, `stddev` | `mean > 0`, `stddev > 0`, `range.max > 0` | int, double |
| `exponential` | `mean` | `mean > 0`, `range.max >= 0` | int, double |
| `zipf` | `exponent` | `exponent > 0` | int only |

`lognormal` takes the mean and standard deviation of the value itself, not of
its logarithm. The receiver converts them to the underlying parameters:

```text
sigma^2 = ln(1 + stddev^2 / mean^2)
mu      = ln(mean) - sigma^2 / 2
```

`exponential` takes the mean; the rate is `1 / mean`.

`zipf` assigns rank 1 to `range.min`, rank 2 to `range.min + 1`, and so on.
Smaller values are the most frequent. Probability of rank `k` is proportional
to `1 / k^exponent`.

## Sampling

### Inverse transform sampling

Continuous specs are sampled with inverse transform sampling over the
truncated distribution. For a distribution with cumulative distribution
function `F` and quantile function `F^-1`, restricted to `[a, b]`:

```text
Fa = F(a)                         # precomputed at registry load
Fb = F(b)                         # precomputed at registry load
u  = uniform random in [0, 1)
x  = F^-1(Fa + u * (Fb - Fa))
```

Properties:

- Every sample lies in `[a, b]`. There is no clamping, so no artificial spike
  of values at the bounds.
- Exactly one uniform random draw and one quantile evaluation per value. There
  is no rejection loop, so cost per value is constant and predictable, which
  matters for a load generator.
- With a seeded RNG the output is fully reproducible.

Per-distribution functions:

| `type` | `F(x)` | `F^-1(p)` |
| --- | --- | --- |
| `uniform` | `(x - min) / (max - min)` | `min + p * (max - min)` |
| `normal` | `0.5 * erfc(-(x - mean) / (stddev * sqrt(2)))` | `mean + stddev * Phi^-1(p)` |
| `lognormal` | normal CDF of `ln(x)` with `mu`, `sigma`; `0` for `x <= 0` | `exp(mu + sigma * Phi^-1(p))` |
| `exponential` | `1 - exp(-x / mean)` for `x >= 0`; `0` otherwise | `-mean * ln(1 - p)` |

`Phi^-1` (the standard normal quantile) has no closed form. It is computed with
a rational approximation (for example Acklam's algorithm, relative error about
`1e-9`), optionally refined with one Halley step for full double precision.

Discrete specs (`values` + `weights`) are also sampled by inverse transform:
the cumulative weights are precomputed and a uniform draw is located with a
binary search (equivalent to `rand::distr::weighted::WeightedIndex`).

### Tail handling

When the range lies far in a tail (for example `normal` with `mean: 0`,
`stddev: 1`, and range `[8, 10]`), `F(a)` and `F(b)` are both extremely close
to `1.0` and their difference loses precision. To avoid this:

- The normal CDF is computed with `erfc` rather than `erf`.
- When the range lies entirely above the mean (or entirely in the right tail),
  sampling is performed on the mirrored distribution using the survival
  function `1 - F`, then mapped back.

### Integer sampling

For `int` attributes with a continuous distribution other than `uniform` or
`zipf`, the receiver samples the continuous distribution over
`[min - 0.5, max + 0.5]` and rounds to the nearest integer. Each integer `k`
then receives probability proportional to `F(k + 0.5) - F(k - 0.5)`, and both
bounds are reachable with their correct weight.

- `uniform` over an int range draws an integer directly in `[min, max]`.
- `zipf` precomputes a cumulative table over `[min, max]` and samples by
  binary search. Ranges larger than a fixed table cap (for example one million
  entries) fall back to a rejection-inversion Zipf sampler.

### Load-time probability check

If the truncated range carries no representable probability mass (`Fb - Fa`
underflows to zero, computed with the tail-safe method), the spec is rejected
at load time. Ranges deep in a tail but with non-zero mass (for example
normal(0, 1) on `[8, 10]`, mass ~6e-16) are accepted because inverse transform
sampling handles them exactly. Example error:

```text
server.port: range [0, 1] has ~0 probability under normal(mean=100, stddev=1)
```

A `mean` outside the range is allowed as long as the range has probability
mass (for example a half-tail shape with `mean: 0` and range `[2, 5]`).

## Attribute types

The same value spec applies to every attribute type. Scalars use it directly.
Arrays and templates nest value specs under fixed keys.

| Attribute type | Discrete form | Continuous form | Extra keys |
| --- | --- | --- | --- |
| `int` | yes | yes | none |
| `double` | yes | yes | none |
| `string` | yes | no | none |
| `boolean` | yes | no | none |
| enum | yes (values must be member values) | no | none |
| `any` | yes (mixed scalar values) | no | none |
| `string[]`, `int[]`, `double[]`, `boolean[]` | via `items` | via `items` (int / double only) | `length`, `items` |
| `template[T]` | via `value` | via `value` (int / double only) | `keys`, `value` |

- `length` is an int value spec (`>= 0`) for the array length. Each array
  element is sampled independently from `items`.
- `keys` is a string value spec. The emitted attribute name is
  `<template prefix>.<key>`, for example `http.request.header.content-type`.
- `value` is the value spec for the template's inner type. For array templates
  it has the same `length` / `items` shape as a plain array.

Values in the discrete form must match the attribute type. YAML integers are
accepted for `double` attributes and converted.

## Signal-level keys

Signal groups accept the following keys under `traffic-generator`. All keys
except `bounds` are value specs.

| Signal | Key | Spec type | Default when absent |
| --- | --- | --- | --- |
| metric (all instruments) | `value` | int / double | Current behavior (`1.0`) |
| metric (all instruments) | `data_points` | int, `>= 1` | `1` |
| metric (histogram) | `observations` | int, `>= 1` | `1` |
| metric (histogram) | `bounds` | list of increasing numbers | OTel SDK default bounds |
| span | `duration` | int / double, milliseconds, `>= 0` | Current behavior (10 microseconds) |
| span | `status` | string, values in `unset`, `ok`, `error` | Status not set |
| event | `severity` | string, `SeverityNumber` names | `INFO` |

### Metrics

- Data point type: metric values are emitted as `as_int` or `as_double`
  according to the existing `code_generation.metric_value_type` annotation on
  the metric group (`int` or `double`). When that annotation is absent the
  value is emitted as a double. For int metrics the `value` spec is sampled
  with the integer rules above.
- `gauge`: each data point value is sampled independently from `value`.
- `updowncounter`: each data point value is sampled independently from
  `value`; negative values are allowed. Emitted as a non-monotonic sum.
- `counter`: `value` is the per-interval increment. Data points are emitted
  with `AggregationTemporality::Delta` and `is_monotonic: true`, so each sample
  is independent and no per-series state is kept across batches. This also
  keeps `pre_generated` mode valid when batches are replayed. `range.min` must
  be `>= 0`.
- `data_points`: the number of data points per emitted metric. Attributes are
  resampled independently for each data point. Duplicate attribute sets are
  possible when the attribute value space is small; this is accepted.
- `histogram`: for each data point the receiver samples `observations` to get
  `n`, then samples `value` `n` times and aggregates the results into
  `bucket_counts`, `count`, `sum`, `min`, and `max` using `bounds`. Bucket
  `i` counts observations `x` with `bounds[i-1] < x <= bounds[i]`, matching the
  OTLP explicit bucket definition. When `bounds` is absent the OTel SDK default
  boundaries are used:

  ```text
  [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]
  ```

### Spans

- `duration` is sampled per span in milliseconds and converted to nanoseconds:
  `end_time = start_time + round(duration * 1_000_000)`.
- `status` is sampled per span and mapped to `Status { code }` (`unset` -> 0,
  `ok` -> 1, `error` -> 2). No status message is set.

### Events (logs)

- `severity` values are `SeverityNumber` short names: `TRACE`, `TRACE2`,
  `TRACE3`, `TRACE4`, `DEBUG` .. `DEBUG4`, `INFO` .. `INFO4`, `WARN` ..
  `WARN4`, `ERROR` .. `ERROR4`, `FATAL` .. `FATAL4`. The sampled name sets both
  `severity_number` and `severity_text`.

## Annotation catalogue

Examples use the v1 registry format (`groups:` with `id:`), which is the format
the receiver resolves to today.

### Placement

```yaml
groups:
  - id: registry.example
    type: attribute_group
    brief: Example attribute definitions.
    attributes:
      # 1. Attribute definition: default for every signal using the attribute
      - id: http.response.status_code
        type: int
        stability: stable
        brief: HTTP response status code.
        annotations:
          traffic-generator:
            values: [200, 404, 500]
            weights: [90, 7, 3]

  - id: span.example.server
    type: span
    span_kind: server
    stability: development
    brief: Example server span.
    # 2. Signal group: signal-level keys
    annotations:
      traffic-generator:
        duration: { range: { min: 1, max: 2000 } }
    attributes:
      # 3. Attribute reference: overrides the definition for this signal only
      - ref: http.response.status_code
        annotations:
          traffic-generator:
            values: [500, 503]
```

### int

```yaml
# Discrete, uniform
- id: example.int.values
  type: int
  annotations:
    traffic-generator:
      values: [80, 443, 8080]

# Discrete, weighted
- id: example.int.weighted
  type: int
  annotations:
    traffic-generator:
      values: [200, 201, 204, 400, 404, 500]
      weights: [70, 5, 5, 5, 10, 5]

# Discrete, constant
- id: example.int.constant
  type: int
  annotations:
    traffic-generator:
      values: [443]

# Continuous, uniform (distribution omitted)
- id: example.int.uniform
  type: int
  annotations:
    traffic-generator:
      range: { min: 1024, max: 65535 }

# Continuous, explicit uniform
- id: example.int.uniform_explicit
  type: int
  annotations:
    traffic-generator:
      range: { min: 0, max: 100 }
      distribution: { type: uniform }

# Continuous, normal
- id: example.int.normal
  type: int
  annotations:
    traffic-generator:
      range: { min: 1024, max: 65535 }
      distribution: { type: normal, mean: 8080, stddev: 500 }

# Continuous, lognormal (e.g. body size in bytes)
- id: example.int.lognormal
  type: int
  annotations:
    traffic-generator:
      range: { min: 1, max: 1048576 }
      distribution: { type: lognormal, mean: 2048, stddev: 4096 }

# Continuous, exponential (e.g. retry count)
- id: example.int.exponential
  type: int
  annotations:
    traffic-generator:
      range: { min: 0, max: 50 }
      distribution: { type: exponential, mean: 2 }

# Continuous, zipf (e.g. tenant id with a few hot tenants)
- id: example.int.zipf
  type: int
  annotations:
    traffic-generator:
      range: { min: 1, max: 1000 }
      distribution: { type: zipf, exponent: 1.1 }
```

### double

```yaml
# Discrete, uniform
- id: example.double.values
  type: double
  annotations:
    traffic-generator:
      values: [0.25, 0.5, 0.75]

# Discrete, weighted
- id: example.double.weighted
  type: double
  annotations:
    traffic-generator:
      values: [0.1, 0.5, 1.0]
      weights: [60, 30, 10]

# Continuous, uniform
- id: geo.location.lon
  type: double
  annotations:
    traffic-generator:
      range: { min: -180.0, max: 180.0 }

# Continuous, normal
- id: example.double.normal
  type: double
  annotations:
    traffic-generator:
      range: { min: 0.0, max: 1.0 }
      distribution: { type: normal, mean: 0.5, stddev: 0.15 }

# Continuous, lognormal (seconds)
- id: example.double.lognormal
  type: double
  annotations:
    traffic-generator:
      range: { min: 0.001, max: 30.0 }
      distribution: { type: lognormal, mean: 0.15, stddev: 0.3 }

# Continuous, exponential
- id: example.double.exponential
  type: double
  annotations:
    traffic-generator:
      range: { min: 0.0, max: 60.0 }
      distribution: { type: exponential, mean: 5.0 }
```

### string

```yaml
# Uniform
- id: example.string
  type: string
  annotations:
    traffic-generator:
      values: [us-east-1, us-west-2, eu-west-1]

# Weighted
- id: example.string.weighted
  type: string
  annotations:
    traffic-generator:
      values: [/api/users, /api/orders, /health]
      weights: [50, 30, 20]

# Constant
- id: example.string.constant
  type: string
  annotations:
    traffic-generator:
      values: [checkout-service]
```

### boolean

```yaml
# Uniform
- id: example.bool
  type: boolean
  annotations:
    traffic-generator:
      values: [true, false]

# Weighted (5% true)
- id: example.bool.weighted
  type: boolean
  annotations:
    traffic-generator:
      values: [true, false]
      weights: [5, 95]

# Constant
- id: example.bool.constant
  type: boolean
  annotations:
    traffic-generator:
      values: [false]
```

### enum

```yaml
# String members, weighted subset
- id: http.request.method
  type:
    members:
      - { id: get, value: GET, stability: stable }
      - { id: post, value: POST, stability: stable }
      - { id: delete, value: DELETE, stability: stable }
  annotations:
    traffic-generator:
      values: [GET, POST]
      weights: [80, 20]

# Int members
- id: example.enum.int
  type:
    members:
      - { id: low, value: 1, stability: development }
      - { id: high, value: 2, stability: development }
  annotations:
    traffic-generator:
      values: [1, 2]
      weights: [9, 1]

# No annotation: all members sampled uniformly
- id: example.enum.default
  type:
    members:
      - { id: a, value: a, stability: development }
      - { id: b, value: b, stability: development }
```

### any

```yaml
# Mixed scalar values (discrete form only)
- id: example.any
  type: any
  annotations:
    traffic-generator:
      values: ["abc", 42, 3.5, true]
      weights: [4, 3, 2, 1]
```

### Arrays

```yaml
# string[]
- id: process.command_args
  type: string[]
  annotations:
    traffic-generator:
      length: { range: { min: 1, max: 4 } }
      items:
        values: [--config=a.yaml, -v, --debug, run]
        weights: [4, 2, 1, 3]

# int[]: fixed length, continuous items
- id: example.int_array
  type: int[]
  annotations:
    traffic-generator:
      length: { values: [3] }
      items:
        range: { min: 0, max: 255 }

# double[]: weighted lengths, normal items
- id: example.double_array
  type: double[]
  annotations:
    traffic-generator:
      length:
        values: [1, 2, 5]
        weights: [5, 3, 2]
      items:
        range: { min: 0.0, max: 10.0 }
        distribution: { type: normal, mean: 5.0, stddev: 2.0 }

# boolean[]
- id: example.bool_array
  type: boolean[]
  annotations:
    traffic-generator:
      length: { range: { min: 1, max: 3 } }
      items:
        values: [true, false]
        weights: [1, 3]
```

### Templates

```yaml
# template[string]
- id: example.template.string
  type: template[string]
  annotations:
    traffic-generator:
      keys: { values: [env, team, region] }
      value: { values: [prod, staging] }

# template[int]
- id: example.template.int
  type: template[int]
  annotations:
    traffic-generator:
      keys: { values: [shard] }
      value: { range: { min: 0, max: 15 } }

# template[double]
- id: example.template.double
  type: template[double]
  annotations:
    traffic-generator:
      keys: { values: [score] }
      value:
        range: { min: 0.0, max: 1.0 }
        distribution: { type: normal, mean: 0.7, stddev: 0.1 }

# template[boolean]
- id: example.template.bool
  type: template[boolean]
  annotations:
    traffic-generator:
      keys: { values: [cached, retried] }
      value:
        values: [true, false]
        weights: [1, 4]

# template[any]
- id: example.template.any
  type: template[any]
  annotations:
    traffic-generator:
      keys: { values: [custom] }
      value: { values: ["x", 1, true] }

# template[string[]]
- id: http.request.header
  type: template[string[]]
  annotations:
    traffic-generator:
      keys:
        values: [content-type, user-agent, accept]
        weights: [5, 3, 2]
      value:
        length: { values: [1] }
        items: { values: [application/json, text/plain] }

# template[int[]]
- id: example.template.int_array
  type: template[int[]]
  annotations:
    traffic-generator:
      keys: { values: [ports] }
      value:
        length: { range: { min: 1, max: 3 } }
        items: { values: [80, 443] }

# template[double[]]
- id: example.template.double_array
  type: template[double[]]
  annotations:
    traffic-generator:
      keys: { values: [weights] }
      value:
        length: { values: [2] }
        items: { range: { min: 0.0, max: 1.0 } }

# template[boolean[]]
- id: example.template.bool_array
  type: template[boolean[]]
  annotations:
    traffic-generator:
      keys: { values: [flags] }
      value:
        length: { range: { min: 1, max: 4 } }
        items:
          values: [true, false]
          weights: [1, 1]
```

### Metric annotations

```yaml
# gauge
- id: metric.example.gauge
  type: metric
  metric_name: example.cpu.utilization
  instrument: gauge
  unit: "1"
  annotations:
    code_generation:
      metric_value_type: double
    traffic-generator:
      value:
        range: { min: 0.0, max: 1.0 }
        distribution: { type: normal, mean: 0.4, stddev: 0.2 }
      data_points: { range: { min: 1, max: 4 } }

# updowncounter
- id: metric.example.updown
  type: metric
  metric_name: example.connections.active
  instrument: updowncounter
  unit: "{connection}"
  annotations:
    code_generation:
      metric_value_type: int
    traffic-generator:
      value: { range: { min: -10, max: 10 } }

# counter (per-interval increment, delta temporality)
- id: metric.example.counter
  type: metric
  metric_name: example.requests
  instrument: counter
  unit: "{request}"
  annotations:
    code_generation:
      metric_value_type: int
    traffic-generator:
      value:
        range: { min: 0, max: 500 }
        distribution: { type: exponential, mean: 50 }

# histogram with explicit bounds
- id: metric.example.histogram
  type: metric
  metric_name: example.request.duration
  instrument: histogram
  unit: s
  annotations:
    code_generation:
      metric_value_type: double
    traffic-generator:
      value:
        range: { min: 0.001, max: 10.0 }
        distribution: { type: lognormal, mean: 0.2, stddev: 0.5 }
      observations: { range: { min: 10, max: 1000 } }
      bounds: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
      data_points: { values: [1, 2] }

# histogram with default bounds
- id: metric.example.histogram_default
  type: metric
  metric_name: example.payload.size
  instrument: histogram
  unit: By
  annotations:
    traffic-generator:
      value: { range: { min: 0, max: 10000 } }
      observations: { values: [100] }
```

### Span annotations

```yaml
- id: span.example.client
  type: span
  span_kind: client
  annotations:
    traffic-generator:
      duration:
        range: { min: 1, max: 5000 }
        distribution: { type: exponential, mean: 120 }
      status:
        values: [unset, ok, error]
        weights: [80, 15, 5]
```

### Event annotations

```yaml
- id: event.example
  type: event
  name: example.event
  annotations:
    traffic-generator:
      severity:
        values: [INFO, WARN, ERROR]
        weights: [90, 8, 2]
```

### Invalid annotations

Each of the following is rejected at registry load time.

```yaml
# values and range are mutually exclusive
{ values: [1, 2], range: { min: 0, max: 5 } }
# distribution requires range
{ values: [1, 2], distribution: { type: uniform } }
# weights requires values
{ range: { min: 0, max: 5 }, weights: [1] }
# weights length must equal values length
{ values: [1, 2], weights: [1] }
# weights must sum to > 0
{ values: [1, 2], weights: [0, 0] }
# min must be <= max
{ range: { min: 5, max: 0 } }
# missing required distribution parameter (stddev)
{ range: { min: 0, max: 5 }, distribution: { type: normal, mean: 2 } }
# zipf only applies to int
{ range: { min: 0.0, max: 1.0 }, distribution: { type: zipf, exponent: 1 } }
# range has ~0 probability under the distribution
{ range: { min: 0, max: 1 }, distribution: { type: normal, mean: 100, stddev: 1 } }
# neither values nor range
{ weights: [1, 2] }
# unknown key (typo of weights)
{ values: [1, 2], weight: [1, 1] }
# unknown key in range
{ range: { min: 0, max: 5, step: 1 } }
# parameter belongs to a different distribution type
{ range: { min: 0, max: 5 }, distribution: { type: normal, mean: 2, stddev: 1, exponent: 1 } }
```

Type-dependent errors:

| Context | Annotation | Error |
| --- | --- | --- |
| `string` attribute | `{ range: { min: 0, max: 5 } }` | Continuous form not allowed for string |
| `int` attribute | `{ values: ["a"] }` | Value type does not match attribute type |
| enum attribute | `{ values: [PATCH] }` | `PATCH` is not a member value |
| array attribute | `{ values: [1, 2] }` | Arrays require `length` and `items` |
| template attribute | `{ values: [x] }` | Templates require `keys` and `value` |
| counter `value` | `{ range: { min: -5, max: 5 } }` | Counter increments must be `>= 0` |
| histogram `bounds` | `[1, 0.5]` | Bounds must be strictly increasing |
| span `status` | `{ values: [failed] }` | Unknown status |
| event `severity` | `{ values: [LOUD] }` | Unknown severity |
| gauge group | `{ observations: { values: [10] } }` | `observations` is only valid on histograms |
| span group | `{ severity: { values: [INFO] } }` | `severity` is only valid on event groups |
| scalar attribute | `{ values: [1], items: { values: [1] } }` | `items` is only valid on array attributes |

## Value precedence

For each attribute value the receiver uses the first source that applies:

1. `traffic-generator` annotation on the signal's attribute reference.
2. `traffic-generator` annotation on the attribute definition.
3. Enum members, sampled uniformly (enum attributes only).
4. `examples`, sampled uniformly (current behavior).
5. Type default (current constants).

Steps 1 and 2 are observed through the single resolved `Attribute.annotations`
field because of Weaver v1 reference semantics (see
[Where annotations are available](#where-annotations-are-available)).

For signal-level keys, the receiver uses the group annotation when present and
the current behavior otherwise.

## Validation

Annotations are parsed and validated once, when the registry is loaded, and
errors are surfaced through `validate_config` and receiver startup. Each error
names the group id, attribute name, and annotation key path, for example:

```text
span.example.client: traffic-generator.status.values[1]: unknown status "failed"
http.request.method: traffic-generator.values[2]: "PATCH" is not an enum member
```

Validation covers:

- Structure: unknown keys per context (see [Unknown keys](#unknown-keys)),
  mutually exclusive keys, required keys, and `weights` length / sign / sum.
- Ranges: `min <= max`, finite bounds, and non-negative where required
  (`length`, `data_points`, `observations`, `duration`, counter `value`).
- Distributions: known `type`, all required parameters present, finite, and in
  their valid domain; `zipf` only on int; the range carries non-zero
  probability mass.
- Types: discrete values match the attribute type; enum values are members;
  arrays and templates use their nested keys.
- Signal keys: `bounds` strictly increasing and finite; `status` and
  `severity` names known; keys valid for the signal type (for example
  `observations` only on histograms).

## Implementation

### Modules

| File | Contents |
| --- | --- |
| `annotation.rs` | Namespace constant, `lookup` of the `traffic-generator` map, hand-written parsers (`parse_value_spec`, `parse_attribute_spec`, `parse_metric_spec`, `parse_span_spec`, `parse_event_spec`) that select the form by key presence, reject unknown keys per context, and report errors with the full key path (`AnnotationError`); `validate_registry` checks every annotation in a resolved registry and returns `RegistryAnnotationError`s (group, attribute, key path) |
| `sampler.rs` | Standard normal functions `normal_cdf`, `normal_sf`, `normal_quantile`, `normal_inverse_sf` (`libm::erfc` plus Acklam's quantile approximation with one Halley step); `NumberSampler` (truncated inverse transform for normal, lognormal, exponential; integer widening and rounding; zipf cumulative table up to `ZIPF_TABLE_CAP` entries, `rand_distr::Zipf` above); `IndexSampler` for weighted / uniform discrete values; `sample_value` (plus `sample_f64`, `sample_count`, `sample_string`) draws one value from a value spec |
| `attributes.rs` | `get_attribute_name_value(attribute, rng)`: annotation, then enum members (uniform), then examples, then type defaults; template names expand to `<prefix>.<key>` |
| `semconv_signal.rs` | `semconv_otlp_metrics` / `semconv_otlp_traces` / `semconv_otlp_logs` iterate the registry groups as before and read each group's annotation for metric values, data points, histogram observations / bounds, span duration / status, and log severity; `*_with_rng` variants take an explicit RNG |

The parser reads the `serde_yaml::Mapping` inside Weaver's `YamlValue`
directly, rather than deserializing into `deny_unknown_fields` structs, so each
error names the exact key path.

### Receiver wiring

- `Config::get_registry()` resolves the registry and runs
  `validate_registry`; invalid annotations fail receiver startup with a list
  of every error.
- `WeaverGenerator` holds the resolved registry and calls `semconv_otlp_*` as
  before. Annotations are looked up, parsed, and sampled each time a value is
  generated; there is no cache. If generation throughput becomes a problem,
  caching parsed specs per group and attribute is the first optimization.
- If an annotation fails to parse during generation (not expected after
  startup validation), the value falls back to enum members, examples, or the
  type default instead of panicking.
- Randomness uses the thread RNG (`rand::rng()`). There is no seed option;
  tests seed `StdRng` directly.
- Metric points are emitted as `as_int` or `as_double` according to
  `code_generation.metric_value_type`. Sums use delta temporality; histograms
  use delta temporality with populated buckets, count, sum, min, and max.

### Dependencies

- `libm` (production CDF math) and `rand_distr` (zipf above the table cap).

### Generation strategy interaction

With `generation_strategy: pre_generated`, batches are built once at startup
and replayed by cloning. All sampled values are therefore fixed after startup;
variety exists only across the pre-generated batches. Delta temporality for
counters keeps replayed batches semantically valid.

### Vendored registry maintenance

`cargo xtask update-semconv` refuses to run while the vendored model carries
`traffic-generator` annotations, unless `--drop-annotations` is passed.
Automatically re-applying annotations after an upgrade is a follow-up (see
[temp-annotation-revisit.md](temp-annotation-revisit.md)).

## Testing

Tests live in `annotation_tests.rs`. They load the vendored registry from disk
(no network access) and seed `StdRng` directly so every run is deterministic.

- Parsing: every value-spec form, every distribution, every attribute type
  (scalars, the four array types, the nine template types, enums), and every
  signal key.
- Rejections: mutual exclusion and pairing rules, weights length / sign / sum,
  `min > max`, non-integer int bounds, non-finite numbers, missing or invalid
  distribution parameters, zipf on doubles, ranges with zero mass, type
  mismatches, enum non-members, unknown keys at every level and in the wrong
  context, bad bounds, unknown status and severity names.
- Range guarantees: every non-uniform method (normal, lognormal, exponential,
  zipf) is sampled as both doubles and ints over centered, wide, left-tail,
  right-tail, negative, mean-outside, and from-zero ranges; every sample must
  be finite and inside `[min, max]`, and int endpoints must be reachable.
- Shape: mean / quantiles of uniform, normal (untruncated and half-truncated),
  lognormal, exponential (including the memoryless tail), and zipf rank
  frequencies; weighted and uniform discrete proportions.
- Normal functions: the CDF, survival function, and quantile match
  high-precision reference values (40-60 digit `mpmath`) from the center out
  to `p = 1e-300`; symmetry, monotonicity, round trips, edge cases, and a
  derivative check against the closed-form density are verified over dense
  grids; lognormal parameter conversion reproduces the requested moments.
- Signals: counters are delta and non-negative, int/double points follow
  `metric_value_type`, histogram buckets / count / sum / min / max are
  consistent, span durations vary, status codes come from the annotation, and
  log severity number and text agree.
- Registry: the vendored registry validates with zero annotation errors,
  `validate_registry` reports every error in a corrupted registry, every
  metric / span / event and every int / double / boolean / int[] attribute is
  annotated, and `SEMCONV_REGISTRY_REFSPEC` matches `vendor/semconv/VERSION`.

## Distribution math

The standard normal CDF, survival function, and quantile are written by hand
in `sampler.rs` on top of `libm` rather than taken from a statistics crate.
`statrs` and `cdflib` were evaluated: the hand-written functions matched
`cdflib` to 1e-12 relative error and were more precise than `statrs` in the
far tails, while adding no dependency beyond `libm` and never panicking or
returning errors on the sampling path. The functions are tested against fixed
high-precision reference values and mathematical identities (see
[Testing](#testing)).
