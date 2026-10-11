//! Telemetry a guest reports: spans and events to AWS X-Ray, metrics to CloudWatch.
//!
//! The ABI is `wit/telemetry/telemetry.wit`. Like every interface the runtime adds, nothing in it
//! takes a tenant id: the serve layer puts a [`TelemetryContext`] on the store before the guest
//! runs, carrying the tenant it has already authenticated.
//!
//! # One trace per invocation, shipped when the store goes
//!
//! A context opens a trace when it is made and hands it to X-Ray when it is dropped — which is
//! when the store holding it is, at the end of the invocation however that ends, a timeout or a
//! trap included. No call site has to remember to flush, so none can forget.
//!
//! # Never in the guest's way
//!
//! Every host call here is synchronous and does no I/O. A span is a node in an in-memory tree, a
//! metric is an entry in a map, and a finished trace goes to a bounded queue by `try_send`. The
//! network belongs to the forwarders ([`xray`], [`metrics`]) on tasks of their own. Telemetry that
//! cannot keep up is dropped and counted, never waited for — the guest-log pipeline's rule (see
//! [`crate::guest_io`]), for the same reason.
//!
//! With X-Ray or metrics off the calls still succeed; they do less. Every event and closed span is
//! also mirrored to the runtime's console, so an emulator with neither still shows what a guest
//! reported.
//!
//! # What leaves the enclave
//!
//! What a guest puts in a span name, a message, a field or a metric is its own business, exactly
//! as its stdout is. The runtime adds only what it already knows: the tenant (an X-Ray annotation,
//! never a metric dimension — CloudWatch bills per unique combination), the kind of invocation,
//! the image's PCR0, the region and its own version.

mod metrics;
mod xray;

pub use metrics::{
    CloudWatchMetrics, Datum, DatumValue, MetricDestination, Metrics, MetricsConfig,
};
pub use xray::{TraceDestination, Tracer, XrayConfig, XrayDestination};

use std::sync::Arc;
use std::time::{Duration, Instant};

use wasmtime::component::{ComponentType, Lift, Linker, Resource, ResourceType};

use crate::state::State;

/// Waits between tries for a failure that may heal, shared by both forwarders. Telemetry is best
/// effort: after these it is dropped and counted rather than held.
const RETRY_BACKOFF: [Duration; 2] = [Duration::from_millis(500), Duration::from_secs(2)];

/// At most one warning per this, however much is being dropped or refused.
const WARN_INTERVAL: Duration = Duration::from_secs(60);

/// How long shutdown waits for the last batch to go.
const FLUSH_DEADLINE: Duration = Duration::from_secs(5);

/// The console target guest telemetry is mirrored under — the guest-log one, so one filter
/// governs everything a guest says.
const GUEST_TARGET: &str = "guest";

// --- The ABI's types ---------------------------------------------------------------------------

/// The WIT `level`.
#[derive(ComponentType, Lift, Clone, Copy, Debug, PartialEq, Eq)]
#[component(enum)]
#[repr(u8)]
pub enum Level {
    #[component(name = "trace")]
    Trace,
    #[component(name = "debug")]
    Debug,
    #[component(name = "info")]
    Info,
    #[component(name = "warn")]
    Warn,
    #[component(name = "error")]
    Error,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Trace => "trace",
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
}

/// The WIT `field`.
#[derive(ComponentType, Lift, Clone, Debug, PartialEq, Eq)]
#[component(record)]
pub struct Field {
    #[component(name = "key")]
    pub key: String,
    #[component(name = "value")]
    pub value: String,
}

/// The WIT `unit`.
#[derive(ComponentType, Lift, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[component(enum)]
#[repr(u8)]
pub enum Unit {
    #[component(name = "none")]
    None,
    #[component(name = "count")]
    Count,
    #[component(name = "seconds")]
    Seconds,
    #[component(name = "milliseconds")]
    Milliseconds,
    #[component(name = "bytes")]
    Bytes,
    #[component(name = "kilobytes")]
    Kilobytes,
    #[component(name = "percent")]
    Percent,
}

/// The WIT `dimension`.
#[derive(ComponentType, Lift, Clone, Debug, PartialEq, Eq)]
#[component(record)]
pub struct Dimension {
    #[component(name = "name")]
    pub name: String,
    #[component(name = "value")]
    pub value: String,
}

/// What a guest's `span` handle refers to: a node in its invocation's trace, or nothing when the
/// span was not recorded (no context, or the trace was already full). Every method on a span that
/// refers to nothing is accepted and does nothing.
pub struct SpanHandle {
    node: Option<usize>,
}

// --- The process-wide half ---------------------------------------------------------------------

/// What started an invocation, recorded on its trace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Invocation {
    /// An inbound request.
    Request,
    /// A background task run.
    Task,
    /// A message on a held connection.
    Message,
}

impl Invocation {
    fn as_str(self) -> &'static str {
        match self {
            Invocation::Request => "request",
            Invocation::Task => "task",
            Invocation::Message => "message",
        }
    }
}

/// Where traces and metrics go, for the whole process. Cheap to clone; the default sends nowhere.
#[derive(Clone, Default)]
pub struct Telemetry {
    tracer: Option<Arc<Tracer>>,
    metrics: Option<Arc<Metrics>>,
}

impl std::fmt::Debug for Telemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Telemetry")
            .field("xray", &self.tracer.is_some())
            .field("metrics", &self.metrics.is_some())
            .finish()
    }
}

impl Telemetry {
    pub fn new(tracer: Option<Arc<Tracer>>, metrics: Option<Arc<Metrics>>) -> Self {
        Telemetry { tracer, metrics }
    }

    /// The context for one invocation, to put on its store before the guest runs.
    pub fn context(&self, tenant: [u8; 16], invocation: Invocation) -> TelemetryContext {
        let tenant = hex::encode(tenant);
        TelemetryContext {
            trace: xray::Trace::new(tenant.clone(), invocation),
            tenant,
            tracer: self.tracer.clone(),
            metrics: self.metrics.clone(),
        }
    }

    /// Flush what is queued and stop. Each part is bounded by its own deadline, so telemetry that
    /// will not go can never be what keeps an enclave from stopping.
    pub async fn shutdown(&self) {
        if let Some(tracer) = &self.tracer {
            tracer.shutdown().await;
        }
        if let Some(metrics) = &self.metrics {
            metrics.shutdown().await;
        }
    }
}

// --- The per-invocation half -------------------------------------------------------------------

/// One invocation's telemetry: its trace, and where that trace and its metrics go.
pub struct TelemetryContext {
    tenant: String,
    trace: xray::Trace,
    tracer: Option<Arc<Tracer>>,
    metrics: Option<Arc<Metrics>>,
}

impl TelemetryContext {
    fn event(&mut self, on: Option<usize>, level: Level, message: String, fields: Vec<Field>) {
        console(
            level,
            &self.tenant,
            "guest event",
            None,
            None,
            &message,
            &fields,
        );
        self.trace.event(on, level, message, fields);
    }

    fn open(&mut self, level: Level, name: String, fields: Vec<Field>) -> Option<usize> {
        self.trace.open(level, name, fields)
    }

    fn set_field(&mut self, node: usize, key: String, value: String) {
        self.trace.set_field(node, key, value);
    }

    fn close(&mut self, node: usize) {
        if let Some((level, name, seconds)) = self.trace.close(node) {
            console(
                level,
                &self.tenant,
                "guest span",
                Some(&name),
                Some(seconds),
                "",
                &[],
            );
        }
    }

    fn record(
        &self,
        kind: metrics::Kind,
        name: String,
        value: f64,
        unit: Unit,
        dimensions: Vec<Dimension>,
    ) {
        if let Some(metrics) = &self.metrics {
            metrics.record(kind, name, value, unit, dimensions);
        }
    }
}

impl Drop for TelemetryContext {
    /// The end of the invocation: ship its trace. Never blocks — see [`Tracer::submit`].
    fn drop(&mut self) {
        if let Some(tracer) = &self.tracer {
            let document = self.trace.finish(tracer.image(), tracer.region());
            tracer.submit(document);
        }
    }
}

/// Mirror one report to the runtime's console.
///
/// The guest's text is only ever a quoted **value**, never the message — the rule
/// [`crate::guest_io::TracingLogSink`] follows, for the same reason: a guest that can choose the
/// message can forge a line that looks like the runtime's own.
fn console(
    level: Level,
    tenant: &str,
    what: &'static str,
    span: Option<&str>,
    seconds: Option<f64>,
    message: &str,
    fields: &[Field],
) {
    let fields: Vec<(&str, &str)> = fields
        .iter()
        .map(|f| (f.key.as_str(), f.value.as_str()))
        .collect();
    let duration_ms = seconds.map(|s| (s * 1000.0).round() as u64);
    macro_rules! at {
        ($level:ident) => {
            tracing::$level!(
                target: GUEST_TARGET,
                tenant,
                span = ?span,
                duration_ms = ?duration_ms,
                guest_message = ?message,
                guest_fields = ?fields,
                "{what}"
            )
        };
    }
    match level {
        Level::Trace => at!(trace),
        Level::Debug => at!(debug),
        Level::Info => at!(info),
        Level::Warn => at!(warn),
        Level::Error => at!(error),
    }
}

// --- Delivery, shared by both forwarders -------------------------------------------------------

/// Why a call to X-Ray or CloudWatch failed, and whether trying again could help.
#[derive(Debug)]
pub enum SendError {
    /// Throttling, a 5xx, a broken connection, an expired session token: worth another go.
    Transient(String),
    /// Refused for good: the role may not write here, or the request itself is malformed.
    Definitive(String),
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Transient(e) | SendError::Definitive(e) => f.write_str(e),
        }
    }
}

/// Sort an SDK failure, the way [`crate::guest_io::cloudwatch`] does for logs.
///
/// Credential errors are deliberately transient: an `UnrecognizedClientException` is usually an
/// expired session token, which heals the moment the SDK refreshes it from IMDS.
fn classify<E, R>(error: aws_smithy_runtime_api::client::result::SdkError<E, R>) -> SendError
where
    E: aws_smithy_types::error::metadata::ProvideErrorMetadata + std::error::Error + 'static,
    R: std::fmt::Debug,
{
    use aws_smithy_types::error::metadata::ProvideErrorMetadata as _;
    let code = error.code().map(str::to_string);
    let message = format!(
        "{}: {}",
        code.as_deref().unwrap_or("unknown"),
        aws_smithy_types::error::display::DisplayErrorContext(&error)
    );
    match code.as_deref() {
        Some(
            "AccessDeniedException"
            | "AccessDenied"
            | "InvalidParameterValue"
            | "InvalidParameterCombination"
            | "MissingParameter"
            | "InvalidRequestException",
        ) => SendError::Definitive(message),
        _ => SendError::Transient(message),
    }
}

/// Whether a rate-limited warning may go out now.
fn warn_due(last: &mut Option<Instant>) -> bool {
    let now = Instant::now();
    if last.is_some_and(|t| now.duration_since(t) < WARN_INTERVAL) {
        return false;
    }
    *last = Some(now);
    true
}

// --- The linker --------------------------------------------------------------------------------

/// The ABI is defined in wit/telemetry/telemetry.wit. No tenant id is accepted.
///
/// Synchronous host functions: nothing here waits on anything, so there is no future to box.
pub fn add_to_linker(linker: &mut Linker<State>) -> wasmtime::Result<()> {
    let mut tracing = linker.instance("enclave:telemetry/tracing@0.1.0")?;

    // Dropping the handle closes the span, if `end` has not already.
    tracing.resource(
        "span",
        ResourceType::host::<SpanHandle>(),
        |mut store, rep| {
            let (ctx, table) = store.data_mut().telemetry_and_table();
            let handle = table.delete(Resource::<SpanHandle>::new_own(rep))?;
            if let (Some(ctx), Some(node)) = (ctx, handle.node) {
                ctx.close(node);
            }
            Ok(())
        },
    )?;

    tracing.func_wrap(
        "event",
        |mut store, (level, message, fields): (Level, String, Vec<Field>)| {
            if let Some(ctx) = store.data_mut().telemetry.as_mut() {
                ctx.event(None, level, message, fields);
            }
            Ok(())
        },
    )?;

    tracing.func_wrap(
        "[constructor]span",
        |mut store, (level, name, fields): (Level, String, Vec<Field>)| {
            let (ctx, table) = store.data_mut().telemetry_and_table();
            let node = ctx.and_then(|ctx| ctx.open(level, name, fields));
            Ok((table.push(SpanHandle { node })?,))
        },
    )?;

    tracing.func_wrap(
        "[method]span.set-field",
        |mut store, (span, key, value): (Resource<SpanHandle>, String, String)| {
            let (ctx, table) = store.data_mut().telemetry_and_table();
            if let (Some(ctx), Some(node)) = (ctx, table.get(&span)?.node) {
                ctx.set_field(node, key, value);
            }
            Ok(())
        },
    )?;

    tracing.func_wrap(
        "[method]span.add-event",
        |mut store,
         (span, level, message, fields): (Resource<SpanHandle>, Level, String, Vec<Field>)| {
            let (ctx, table) = store.data_mut().telemetry_and_table();
            if let (Some(ctx), Some(node)) = (ctx, table.get(&span)?.node) {
                ctx.event(Some(node), level, message, fields);
            }
            Ok(())
        },
    )?;

    tracing.func_wrap(
        "[method]span.end",
        |mut store, (span,): (Resource<SpanHandle>,)| {
            let (ctx, table) = store.data_mut().telemetry_and_table();
            if let (Some(ctx), Some(node)) = (ctx, table.get(&span)?.node) {
                ctx.close(node);
            }
            Ok(())
        },
    )?;

    let mut metrics = linker.instance("enclave:telemetry/metrics@0.1.0")?;

    metrics.func_wrap(
        "counter-add",
        |store, (name, value, dimensions): (String, f64, Vec<Dimension>)| {
            if let Some(ctx) = store.data().telemetry.as_ref() {
                ctx.record(metrics::Kind::Counter, name, value, Unit::Count, dimensions);
            }
            Ok(())
        },
    )?;

    metrics.func_wrap(
        "gauge-set",
        |store, (name, value, unit, dimensions): (String, f64, Unit, Vec<Dimension>)| {
            if let Some(ctx) = store.data().telemetry.as_ref() {
                ctx.record(metrics::Kind::Gauge, name, value, unit, dimensions);
            }
            Ok(())
        },
    )?;

    metrics.func_wrap(
        "histogram-record",
        |store, (name, value, unit, dimensions): (String, f64, Unit, Vec<Dimension>)| {
            if let Some(ctx) = store.data().telemetry.as_ref() {
                ctx.record(metrics::Kind::Histogram, name, value, unit, dimensions);
            }
            Ok(())
        },
    )?;

    Ok(())
}
