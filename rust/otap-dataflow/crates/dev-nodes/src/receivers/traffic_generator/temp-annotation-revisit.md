# Temporary: annotations to revisit

Status: temporary review list. Delete once the items below are resolved.

The `traffic-generator` annotations in `vendor/semconv/model` were added in
bulk from each definition's type, unit, instrument, name, and examples. This
file lists the places where the chosen values are guesses or where the
registry did not give enough information. Everything not listed here follows
the rules in [Annotation rules applied](#annotation-rules-applied).

## Coverage

| Target | Annotated | Notes |
| --- | --- | --- |
| int / double / boolean / int[] attributes | 162 (all) | Includes deprecated attributes |
| Selected string attributes | 19 | See [Strings and enums](#strings-and-enums) |
| enum attributes without examples | 61 | Enums that had neither examples nor an annotation; see [Strings and enums](#strings-and-enums) |
| Metrics (v1 groups + v2 `metrics:`) | 541 (all) | Includes deprecated metrics |
| Spans (v1 groups + v2 `spans:`) | 78 (all) | |
| Events | 32 (all) | |

Not annotated (falls back to uniform examples / enum members): all other
string, string[], template, and `any` attributes; enum attributes that already
carry `examples` (they sample their members uniformly); the single skipped enum
`user_agent.synthetic.type`; and every v2 `*_refinements` entry (see
[Refinements](#refinements)).

The `vendored_registry_annotation_coverage` test enforces annotations only on
signal groups and int / double / boolean / int[] attributes; it does not
require enum coverage, so the example-backed enums above remain valid but
unweighted.

## Metric values that are guesses

| Metric | Annotation | Concern |
| --- | --- | --- |
| `hw.physical_disk.smart` | normal(90, 10) on [0, 100] | Unit is `1` but the value is a raw SMART attribute value, not a ratio |
| `hw.voltage`, `hw.voltage.limit`, `hw.voltage.nominal` | normal(12, 0.5) on [0, 240] | Real value depends on the rail (3.3 V, 5 V, 12 V, mains); one distribution cannot cover all sensors |
| `jvm.system.cpu.load_1m` (`{run_queue_item}`) | lognormal(20, 40) on [0, 1000] | Load average is usually 0-2x CPU count; a smaller range may be more realistic |
| `k8s.service.endpoint.count` (`{endpoint}`) | lognormal(20, 40) on [0, 1000] | Generic count guess |
| `k8s.service.load_balancer.ingress.count` (`{ingress}`) | lognormal(20, 40) on [0, 1000] | Usually 0-2; generic count guess is too high |
| `v8js.resource.active` (`{resource}`) | lognormal(20, 40) on [0, 1000] | Generic count guess |
| `vcs.ref.lines_delta` (`{line}`) | lognormal(20, 40) on [0, 1000] | Diffs can be much larger |
| `vcs.ref.revisions_delta` (`{revision}`) | lognormal(20, 40) on [0, 1000] | Generic count guess |
| `vcs.contributor.count` (`{contributor}`) | lognormal(20, 40) on [0, 1000] | Generic count guess |

## Metric rules that may need tuning

- **Host-dependent sizes.** All `By` updowncounters (106 metrics: memory,
  filesystem, k8s, container usage/limits) share lognormal(512 MiB, 1 GiB) on
  [0, 64 GiB]. Memory limits vs. usage vs. filesystem capacity have very
  different realistic ranges.
- **Kubernetes object counts.** All `{pod}`, `{node}`, `{container}`, `{job}`,
  `{namespace}` updowncounters use normal(20, 15) on [0, 200]. Cluster size is
  deployment-specific.
- **CPU counts.** `{cpu}` updowncounters use normal(4, 4) on [0, 64] and gauges
  normal(4, 2) on [0, 128].
- **Counters.** Counters are emitted as delta increments. Generic counters use
  exponential means of 10 (default), 100 (high-volume units such as
  `{packet}`, `{message}`, `{span}`), and 1 (error-like units). Seconds-based
  counters (`*.cpu.time`, `*.io_time`) use exponential(0.5 s) increments.
- **`hw.status`, `hw.network.up`** (updowncounter, unit `1`) use weighted
  `values: [0, 1]`, since they are 0/1 state flags.
- **Histogram bounds.** Bounds come from the upstream docs where they exist
  (HTTP: `[0.005 .. 10]`, DB/DNS/GC: `[0.001 .. 10]`, gen_ai: `[0.01 ..
  81.92]`, tokens: powers of 4). Others (MCP, messaging, RPC, aspnetcore,
  otel.sdk, faas) reuse the HTTP bounds, and long-lived connection/session
  durations use a `[0.01 .. 3600]` set. The upstream docs do not specify
  bounds for these.
- **`ms` histograms** (`db.client.connections.*`, `rpc.*.duration`) reuse the
  seconds distributions scaled by 1000.
- **Histogram observations / data points.** Every histogram uses
  `observations: [10, 100]` uniform and every metric uses
  `data_points: [1, 2, 3]` weighted `[6, 3, 1]`. These control payload size
  and may need to be tuned for benchmarks.

## Spans

Durations are lognormal by span category (DB ~15 ms, HTTP client ~120 ms,
HTTP server ~80 ms, RPC ~40 ms, gen_ai ~2.5 s, messaging ~10-50 ms, faas
~200 ms, CI/CD pipeline ~5 min, CLI ~0.5 s, .NET network internals 5-30 ms).
Status weights depend only on span kind (`unset` dominant, `error` 2-5%).

- No span fell back to a generic guess, but every value is an estimate.
- `span.cicd.pipeline.run.server` and `span.cicd.pipeline.task.internal` have
  very long durations (minutes); confirm this is desired for load tests.

## Events

- Exception events (`*.exception`): ERROR 80 / WARN 15 / FATAL 5.
- `app.crash`: FATAL 70 / ERROR 30. `app.jank`: WARN 70 / INFO 30.
- `azure.resource.log` / `az.resource.log`: INFO/WARN/ERROR/DEBUG mix.
- `gen_ai.*` events: INFO 80 / DEBUG 20. `rpc.message`: DEBUG / TRACE / INFO.
- Everything else: INFO 90 / DEBUG 10.

## Attributes worth reviewing

| Attribute | Annotation | Concern |
| --- | --- | --- |
| `process.pid` and related pids | lognormal on [1, 4194304] | Linux pid_max; other OSes differ |
| `messaging.kafka.offset`, `messaging.kafka.message.offset` | uniform [0, 1e8] | Offsets are monotonic per partition in reality |
| `messaging.*.enqueued_time`, `messaging.rocketmq.message.delivery_timestamp` | uniform over 2023-2030 epoch range | Not correlated with the generated span time |
| `gen_ai.request.seed`, `gen_ai.openai.request.seed` | uniform [0, 1e6] | Arbitrary |
| `geo.location.lat` | normal(30, 25) on [-90, 90] | Biased to populated latitudes; `lon` is uniform |
| `thread.id` | zipf(1.1) on [1, 10000] | Favors small ids |
| `cpu.logical_number` | zipf(0.8) on [0, 63] | Favors low CPU numbers |
| `pprof.scope.sample_type_order` | length 4, items uniform [0, 3] | Should be a permutation; values may repeat |
| `host.cpu.cache.l2.size` | weighted discrete sizes | Values chosen from common CPUs |
| `faas.max_memory` | weighted 128 MiB - 2 GiB | Common Lambda sizes |

## Strings and enums

Weighted annotations were added only where weighting adds realism:
`http.request.method`, `db.system.name`, `messaging.system`,
`messaging.operation.type`, `rpc.system`, `rpc.system.name`,
`network.transport`, `network.type`, `network.protocol.version`,
`url.scheme`, `otel.status_code`, `host.arch`, `os.type`, `cloud.provider`,
`log.iostream`, `gen_ai.operation.name`, `gen_ai.provider.name`.

- Several of these list a subset of enum members (for example
  `db.system.name` lists 10 of 41). Members not listed are never generated.
- `network.protocol.version` and `url.scheme` are plain strings; the
  annotation replaces the upstream examples (`ftp`, `telnet` are dropped).
- Specific spans constrain some of these values in prose (for example
  `span.db.postgresql.client` requires `db.system.name = postgresql`). Weaver
  v1 ref annotations would be needed to pin them per span; not done yet.

A later pass annotated the 61 enum attributes that previously had neither an
annotation nor examples (these fell back to uniform sampling over all members).
This covered enums across active and deprecated namespaces (for example
`faas.trigger`, `hw.type`, `cloud.platform`, `geo.continent.code`,
`telemetry.sdk.language`, the eight `gcp.apphub*` criticality/environment
attributes, and deprecated enums such as `db.system`, `gen_ai.system`,
`rpc.grpc.status_code`). The weights are estimates and bias toward the common
case; several use a weighted subset of members, so unlisted members are never
generated. `rpc.grpc.status_code` uses integer member values.
`user_agent.synthetic.type` was deliberately skipped: its only members are `bot`
and `test`, so any weighting would make every emitted value look like synthetic
traffic. Enums that already carry `examples` were left as-is (they still sample
members uniformly); annotating those for realistic weighting is possible future
work.

## Refinements

Weaver v2 `metric_refinements` and `span_refinements` (hardware status/error
metrics, messaging system spans) are not annotated. It is unclear whether
annotations on a refinement replace or merge with the refined signal's
annotations after resolution; the resolved groups currently inherit the base
signal's annotation (`hw.status`, `hw.errors`, `messaging.*` spans). Verify
the resolved behavior before adding refinement-specific annotations.

## `update-semconv` and annotation injection

`cargo xtask update-semconv` now refuses to run when `vendor/semconv/model`
contains `traffic-generator` annotations (override with
`--drop-annotations`). Re-applying annotations after an upgrade is not
automated yet. Proposed design to revisit:

1. Before deleting `model/`, walk every YAML file and extract each
   `traffic-generator` block keyed by its owner: attribute `id`/`key`, group
   `id`, v2 metric `name`, or v2 span `type`.
2. Store the blocks in a temporary JSON file (`target/`), along with the
   original file path.
3. Copy the new upstream `model/`.
4. For each extracted block, locate the owner in the new tree and insert the
   block with line-based editing (the same approach used to add the
   annotations) so upstream formatting and comments are preserved. If the
   owner already has an `annotations:` map, add the block under it; otherwise
   create the map.
5. Report owners that no longer exist upstream (renamed or removed) and exit
   non-zero so they are re-annotated by hand.

Open points: how to handle owners that moved between v1 and v2 files, and
whether to keep annotations in a separate overlay file instead of inline.

## Annotation rules applied

The full specification is in
[traffic_generator_annotation.md](traffic_generator_annotation.md). The bulk
annotation followed these defaults:

- Ports: ephemeral ports uniform [1024, 65535]; server ports weighted toward
  443/80/8080.
- Sizes in bytes: lognormal with a KiB-scale mean, truncated at 1 MiB (or
  larger for files and memory).
- Counts: exponential with a small mean.
- Ratios and utilization (`1`): normal around 0.4 on [0, 1].
- Booleans: weighted toward the common case.
- Status codes: weighted toward success.
