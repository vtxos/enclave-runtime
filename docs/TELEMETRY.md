# Telemetry

A guest can report what it is doing beyond its stdout: **spans and events to AWS
X-Ray**, and **metrics to CloudWatch**. The ABI is
[`wit/telemetry/telemetry.wit`](../wit/telemetry/telemetry.wit); the runtime's
side is [`runtime/src/telemetry`](../runtime/src/telemetry).

As with every interface the runtime adds, nothing in it takes a tenant id. The
runtime already knows which tenant an invocation belongs to, and attributes what
the guest reports to it.

## From a guest

Include the world, and call it:

```wit
world app {
    include enclave:telemetry/observed@0.1.0;
}
```

```rust
use bindings::enclave::telemetry::{metrics, tracing::{self, Field, Level, Span}};

let span = Span::new(Level::Info, "sign", &[]);       // a subsegment of the invocation
span.set_field("session", &session_id);
tracing::event(Level::Warn, "retrying", &[]);           // lands on the innermost open span
drop(span);                                             // or span.end()

metrics::counter_add("signatures", 1.0, &[]);
metrics::histogram_record("sign-latency", ms, metrics::Unit::Milliseconds, &[]);
```

Every call returns nothing and never fails. When X-Ray or metrics are switched
off, the calls succeed and do less, so a guest never branches on whether
telemetry is on. Each event and closed span is also mirrored to the runtime's
console under the `guest` target, so an emulator with neither still shows what a
guest reported.

## What happens to it

**One trace per invocation.** An inbound request, a task run and a stream
message are each one X-Ray trace. Its root segment is named `enclave-guest` and
carries two *annotations*, the `tenant` (hex) and the `invocation` kind, which
X-Ray indexes, so "every trace for this tenant" is a filter expression. Each span
a guest opens is a subsegment of whichever span was open when it was created;
its level is an annotation. Events and fields ride in the span's `metadata`.

The trace is shipped when the invocation's store is dropped. That is the end of
the invocation however it ends, a timeout or a trap included. Spans the guest
never closed are closed then.

**Metrics are aggregated, not queued.** Over each 30-second window a counter is
summed, a gauge keeps its last value, and a histogram keeps each distinct value
with its count. That is CloudWatch's own `values`/`counts` distribution, so
percentiles survive aggregation. Each window becomes one `PutMetricData` call
per 1000 datums, under the deployment's namespace. The runtime adds no tenant
dimension: CloudWatch bills per unique combination, and that is the runtime's
cost to decide, not a guest's.

## What leaves the enclave

What a guest puts in a span name, a message, a field or a metric is its own
business, exactly as its stdout already is. The runtime adds only what it knows:
the tenant id, the invocation kind, the image's PCR0, the region and its own
version. All of it goes to the operator's own AWS account, the one guest logs
already go to.

Whether an enclave exports traces or metrics at all is part of its image (below),
so it is measured into PCR0. A client can tell from an attestation whether its
guest's traces leave the box.

## Limits

A guest that reports too much loses detail. The guest's calls never fail.

| | Limit | Past it |
|---|---|---|
| Spans per invocation | 256 | counted (`dropped_spans`), not recorded |
| Events per span | 64 | counted (`dropped_events`) |
| Fields per span or event | 32 | the rest ignored |
| Field key / message or value | 64 / 1024 characters | clipped |
| Span name | 200 characters of what X-Ray accepts | clipped; other characters become `_` |
| Trace document | 64 KiB (X-Ray's limit) | events and fields shed, then subsegments; marked `trimmed` |
| Metric name | 1-255 printable ASCII | dropped and counted |
| Dimensions | ≤10, each name and value 1-64 of `[A-Za-z0-9_.-]`, no name twice | dropped and counted |
| Metric value | finite, and 0 or within CloudWatch's range | dropped and counted |
| Series per window | 1000 | new series dropped; existing ones still count |
| Distinct histogram values | 150 per datum | sealed into another datum, not lost |

The metric checks run when a value is recorded, not when it is sent. CloudWatch
refuses a whole request over one bad datum, so letting one through would lose
every other metric in the same call.

## Delivery

Best effort, like guest logs, and for the same reason: nothing a guest calls
waits on the network. A finished trace goes to a bounded queue (1024) by
`try_send`. A forwarder ships batches of up to 50 documents or 1 MiB, after a
one-second linger. A failure that may heal (throttling, a 5xx, an expired session
token) is retried twice, after 0.5 s and 2 s. One that cannot (access denied, a
malformed request) is dropped. Drops are counted and reported at most once a
minute. At shutdown the queue and the last metrics window are flushed within five
seconds.

## Turning it on

In the image, [`deploy/nix/deployment.nix`](../deploy/nix/deployment.nix):

```nix
xray = true;                       # each invocation as an X-Ray trace
metricsNamespace = "Merlin/Guest"; # or "" for no metrics; never under AWS/
```

The parent's role needs the grants. In [`deploy/tofu`](../deploy/tofu):

```hcl
telemetry = { xray = true, metrics_namespace = "Merlin/Guest" }
```

That allows `xray:PutTraceSegments`, which X-Ray cannot scope to a resource, and
`cloudwatch:PutMetricData` for that namespace only. Without the grants the
runtime still serves, and logs at most once a minute that X-Ray or CloudWatch
refused it.

The flags behind these are `ENCLAVE_XRAY` and `ENCLAVE_METRICS_NAMESPACE`. A
malformed namespace stops the boot. A client that cannot be built at startup
does not: that half of telemetry stays on the console. For an emulator,
`ENCLAVE_XRAY_ENDPOINT` and `ENCLAVE_METRICS_ENDPOINT` point the clients at a
stand-in such as LocalStack.

## Testing

`runtime/tests/telemetry.rs` serves the example guest's `/telemetry` route
through the real linker, with X-Ray and CloudWatch replaced by in-memory
destinations. It checks the trace (attribution, nesting, fields, events, timings)
and the metrics (aggregation, units, no tenant dimension):

```console
$ scripts/build-guest.sh http
$ cargo test -p enclave-runtime --features testing --test telemetry -- --ignored
```
