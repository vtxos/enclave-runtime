//! Guest spans as AWS X-Ray traces: the segment tree for one invocation, and the forwarder that
//! ships finished ones with `PutTraceSegments`.
//!
//! The forwarder is the guest-log forwarder in miniature (see [`crate::guest_io::cloudwatch`]): a
//! bounded queue a guest call never waits on, batches sealed by size or by a linger timer, a few
//! backed-off retries for what may heal, and drops counted and reported in aggregate rather than
//! one line each.
//!
//! # One document per invocation
//!
//! The invocation is the root segment; every span a guest opens is a subsegment of whichever span
//! was open when it was created, so spans nest by the order a guest opens them. Events have no
//! type of their own in X-Ray and ride in the (sub)segment's `metadata`, with the span's fields.
//! The tenant, the kind of invocation and a span's level are **annotations**, which X-Ray indexes —
//! so "every trace for this tenant" or "every error span" is a filter expression, not a scan.
//!
//! X-Ray caps a document at 64 KiB. The caps below keep a trace near that by construction, and
//! [`Trace::finish`] sheds detail — events and fields first, then the subsegments — rather than
//! send something X-Ray would refuse whole.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tokio::sync::{mpsc, Notify};

use super::{
    classify, warn_due, Field, Invocation, Level, SendError, FLUSH_DEADLINE, RETRY_BACKOFF,
};

/// X-Ray's ceiling on one segment document.
const MAX_DOCUMENT_BYTES: usize = 64 * 1024;
/// Spans one invocation may open; past this they are counted, not recorded.
const MAX_SPANS: usize = 256;
/// Events one span (or the invocation itself) may carry.
const MAX_EVENTS: usize = 64;
/// Fields one span or event may carry.
const MAX_FIELDS: usize = 32;
/// A field key, in characters.
const MAX_KEY_CHARS: usize = 64;
/// A message or field value, in characters.
const MAX_TEXT_CHARS: usize = 1024;
/// X-Ray's limit on a (sub)segment name.
const MAX_NAME_CHARS: usize = 200;

/// Every root segment's name, so X-Ray's service map shows the guest as one service. What kind of
/// invocation a trace is lives in an annotation.
const SERVICE_NAME: &str = "enclave-guest";

/// Finished traces waiting to be shipped. Past this, new ones are dropped and counted.
const QUEUE_CAPACITY: usize = 1024;
/// Documents per `PutTraceSegments` call: the batch the X-Ray daemon itself sends.
const MAX_BATCH_DOCUMENTS: usize = 50;
/// Bytes per call, well inside the request limit.
const MAX_BATCH_BYTES: usize = 1024 * 1024;
/// How long a partial batch waits for company.
const BATCH_LINGER: Duration = Duration::from_secs(1);

/// Where the X-Ray client sends.
#[derive(Clone, Debug)]
pub struct XrayConfig {
    pub region: String,
    /// A stand-in for the service, for an emulator. `None` is the real one.
    pub endpoint: Option<String>,
}

// --- The trace ---------------------------------------------------------------------------------

/// How much of a trace to render; [`Trace::finish`] steps down until it fits.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Detail {
    /// Everything.
    Full,
    /// The span tree and its timings, without events or fields.
    Structure,
    /// The invocation alone.
    Root,
}

struct Node {
    id: String,
    name: String,
    level: Level,
    /// Index of the enclosing node; the root's is itself and never read.
    parent: usize,
    /// Epoch seconds, as X-Ray wants them.
    start: f64,
    end: Option<f64>,
    fields: Vec<(String, String)>,
    events: Vec<Event>,
}

struct Event {
    at: f64,
    level: Level,
    message: String,
    fields: Vec<(String, String)>,
}

/// One invocation's spans, as an X-Ray segment tree.
pub(super) struct Trace {
    trace_id: String,
    tenant: String,
    invocation: Invocation,
    /// The wall clock is read once, when the trace opens; every later time is that plus a
    /// monotonic offset, so a clock step mid-invocation cannot put a span's end before its start.
    opened_at: f64,
    opened: Instant,
    /// `nodes[0]` is the invocation's root segment.
    nodes: Vec<Node>,
    /// Spans still open, innermost last. Never holds the root.
    open: Vec<usize>,
    dropped_spans: u32,
    dropped_events: u32,
}

impl Trace {
    pub(super) fn new(tenant: String, invocation: Invocation) -> Self {
        let opened_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let root = Node {
            id: random_hex(8),
            name: SERVICE_NAME.to_string(),
            level: Level::Info,
            parent: 0,
            start: opened_at,
            end: None,
            fields: Vec::new(),
            events: Vec::new(),
        };
        Trace {
            // X-Ray's format: version, the start in epoch seconds as 8 hex digits, 96 random bits.
            trace_id: format!("1-{:08x}-{}", opened_at as u64 as u32, random_hex(12)),
            tenant,
            invocation,
            opened_at,
            opened: Instant::now(),
            nodes: vec![root],
            open: Vec::new(),
            dropped_spans: 0,
            dropped_events: 0,
        }
    }

    fn now(&self) -> f64 {
        self.opened_at + self.opened.elapsed().as_secs_f64()
    }

    /// Open a span beneath the innermost open one. `None` when the trace is full.
    pub(super) fn open(&mut self, level: Level, name: String, fields: Vec<Field>) -> Option<usize> {
        if self.nodes.len() > MAX_SPANS {
            self.dropped_spans += 1;
            return None;
        }
        let node = Node {
            id: random_hex(8),
            name: span_name(&name),
            level,
            parent: self.open.last().copied().unwrap_or(0),
            start: self.now(),
            end: None,
            fields: clip_fields(fields),
            events: Vec::new(),
        };
        self.nodes.push(node);
        let index = self.nodes.len() - 1;
        self.open.push(index);
        Some(index)
    }

    pub(super) fn set_field(&mut self, node: usize, key: String, value: String) {
        if let Some(node) = self.nodes.get_mut(node) {
            if node.fields.len() < MAX_FIELDS {
                node.fields
                    .push((clip(key, MAX_KEY_CHARS), clip(value, MAX_TEXT_CHARS)));
            }
        }
    }

    /// An event on `on`, or on the innermost open span, or on the invocation itself.
    pub(super) fn event(
        &mut self,
        on: Option<usize>,
        level: Level,
        message: String,
        fields: Vec<Field>,
    ) {
        let at = self.now();
        let target = on.or(self.open.last().copied()).unwrap_or(0);
        let full = self
            .nodes
            .get(target)
            .is_none_or(|n| n.events.len() >= MAX_EVENTS);
        if full {
            self.dropped_events += 1;
            return;
        }
        self.nodes[target].events.push(Event {
            at,
            level,
            message: clip(message, MAX_TEXT_CHARS),
            fields: clip_fields(fields),
        });
    }

    /// Close a span. `Some` the first time only, with its level, name and length in seconds.
    pub(super) fn close(&mut self, node: usize) -> Option<(Level, String, f64)> {
        if node == 0 {
            return None;
        }
        let now = self.now();
        let span = self.nodes.get_mut(node)?;
        if span.end.is_some() {
            return None;
        }
        span.end = Some(now);
        let closed = (span.level, span.name.clone(), now - span.start);
        self.open.retain(|&i| i != node);
        Some(closed)
    }

    /// Close whatever is still open and render the trace as one segment document, shedding detail
    /// until it fits under X-Ray's limit.
    pub(super) fn finish(&mut self, image: Option<&str>, region: &str) -> String {
        let now = self.now();
        for node in &mut self.nodes {
            node.end.get_or_insert(now);
        }
        self.open.clear();
        let mut document = String::new();
        for detail in [Detail::Full, Detail::Structure, Detail::Root] {
            document = self.render(image, region, detail).to_string();
            if document.len() <= MAX_DOCUMENT_BYTES {
                break;
            }
        }
        document
    }

    fn render(&self, image: Option<&str>, region: &str, detail: Detail) -> Value {
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); self.nodes.len()];
        for (index, node) in self.nodes.iter().enumerate().skip(1) {
            children[node.parent].push(index);
        }

        let root = &self.nodes[0];
        let mut guest = Map::new();
        if detail == Detail::Full && !root.events.is_empty() {
            guest.insert("events".into(), events_json(&root.events));
        }
        if self.dropped_spans > 0 {
            guest.insert("dropped_spans".into(), self.dropped_spans.into());
        }
        if self.dropped_events > 0 {
            guest.insert("dropped_events".into(), self.dropped_events.into());
        }
        if detail != Detail::Full {
            guest.insert("trimmed".into(), true.into());
        }

        let mut document = json!({
            "name": root.name,
            "id": root.id,
            "trace_id": self.trace_id,
            "start_time": root.start,
            "end_time": root.end.unwrap_or(root.start),
            "annotations": {
                "tenant": self.tenant,
                "invocation": self.invocation.as_str(),
            },
            "metadata": {
                "enclave": {
                    "image": image,
                    "region": region,
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "guest": guest,
            },
        });
        if detail != Detail::Root && !children[0].is_empty() {
            document["subsegments"] = children[0]
                .iter()
                .map(|&child| self.subsegment(child, &children, detail))
                .collect();
        }
        document
    }

    fn subsegment(&self, index: usize, children: &[Vec<usize>], detail: Detail) -> Value {
        let node = &self.nodes[index];
        let mut subsegment = json!({
            "id": node.id,
            "name": node.name,
            "start_time": node.start,
            "end_time": node.end.unwrap_or(node.start),
            "annotations": { "level": node.level.as_str() },
        });
        if detail == Detail::Full {
            let mut guest = Map::new();
            if !node.fields.is_empty() {
                guest.insert("fields".into(), fields_json(&node.fields));
            }
            if !node.events.is_empty() {
                guest.insert("events".into(), events_json(&node.events));
            }
            if !guest.is_empty() {
                subsegment["metadata"] = json!({ "guest": guest });
            }
        }
        if !children[index].is_empty() {
            subsegment["subsegments"] = children[index]
                .iter()
                .map(|&child| self.subsegment(child, children, detail))
                .collect();
        }
        subsegment
    }
}

fn fields_json(fields: &[(String, String)]) -> Value {
    Value::Object(
        fields
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect(),
    )
}

fn events_json(events: &[Event]) -> Value {
    events
        .iter()
        .map(|e| {
            json!({
                "at": e.at,
                "level": e.level.as_str(),
                "message": e.message,
                "fields": fields_json(&e.fields),
            })
        })
        .collect()
}

/// A name X-Ray accepts: letters, digits, whitespace and `_ . : / % & # = + \ - @`, at most 200.
fn span_name(name: &str) -> String {
    let name: String = name
        .chars()
        .take(MAX_NAME_CHARS)
        .map(|c| {
            if c.is_alphanumeric() || c.is_whitespace() || "_.:/%&#=+\\-@".contains(c) {
                c
            } else {
                '_'
            }
        })
        .collect();
    if name.trim().is_empty() {
        "span".to_string()
    } else {
        name
    }
}

fn clip(mut text: String, chars: usize) -> String {
    if let Some((cut, _)) = text.char_indices().nth(chars) {
        text.truncate(cut);
    }
    text
}

fn clip_fields(fields: Vec<Field>) -> Vec<(String, String)> {
    fields
        .into_iter()
        .take(MAX_FIELDS)
        .map(|f| (clip(f.key, MAX_KEY_CHARS), clip(f.value, MAX_TEXT_CHARS)))
        .collect()
}

/// An id X-Ray only needs to be unique within a trace — not a secret.
fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    if getrandom::fill(&mut buf).is_err() {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let n = NEXT.fetch_add(1, Ordering::Relaxed).to_le_bytes();
        for (i, b) in buf.iter_mut().enumerate() {
            *b = n[i % 8] ^ i as u8;
        }
    }
    hex::encode(buf)
}

// --- Shipping ----------------------------------------------------------------------------------

/// Where finished segment documents go. A trait so batching and retries can be tested without
/// HTTP, and so the SDK client is reachable from one place.
#[async_trait]
pub trait TraceDestination: Send + Sync {
    /// Ship the documents. `Ok(n)` is how many X-Ray accepted the call but refused.
    async fn put(&self, documents: Vec<String>) -> Result<usize, SendError>;
}

/// Ships finished traces, on a task of its own.
pub struct Tracer {
    image: Option<String>,
    region: String,
    queue: mpsc::Sender<String>,
    dropped: Arc<AtomicU64>,
    stop: Arc<Notify>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Tracer {
    /// Start shipping to `destination`. `image` (PCR0, hex) and `region` go on every trace.
    pub fn start(
        destination: Arc<dyn TraceDestination>,
        image: Option<String>,
        region: String,
    ) -> Arc<Self> {
        let (queue, received) = mpsc::channel(QUEUE_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(forward(
            destination,
            received,
            stop.clone(),
            dropped.clone(),
        ));
        Arc::new(Tracer {
            image,
            region,
            queue,
            dropped,
            stop,
            task: Mutex::new(Some(task)),
        })
    }

    pub(super) fn image(&self) -> Option<&str> {
        self.image.as_deref()
    }

    pub(super) fn region(&self) -> &str {
        &self.region
    }

    /// Hand a finished document to the forwarder. `try_send`, never `send`: this runs as an
    /// invocation's store is dropped, and waiting here is exactly the stall this design refuses.
    pub(super) fn submit(&self, document: String) {
        if self.queue.try_send(document).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Traces dropped so far, whether the queue was full or delivery failed.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Ship what is queued and stop, within [`FLUSH_DEADLINE`].
    pub async fn shutdown(&self) {
        self.stop.notify_one();
        let task = self
            .task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(task) = task {
            let _ = tokio::time::timeout(FLUSH_DEADLINE, task).await;
        }
    }
}

#[derive(Default)]
struct Batch {
    documents: Vec<String>,
    bytes: usize,
}

impl Batch {
    fn fits(&self, document: &str) -> bool {
        self.documents.len() < MAX_BATCH_DOCUMENTS && self.bytes + document.len() <= MAX_BATCH_BYTES
    }

    fn push(&mut self, document: String) {
        self.bytes += document.len();
        self.documents.push(document);
    }

    fn take(&mut self) -> Vec<String> {
        self.bytes = 0;
        std::mem::take(&mut self.documents)
    }
}

async fn forward(
    destination: Arc<dyn TraceDestination>,
    mut received: mpsc::Receiver<String>,
    stop: Arc<Notify>,
    dropped: Arc<AtomicU64>,
) {
    let mut batch = Batch::default();
    let mut linger: Option<tokio::time::Instant> = None;
    let mut warned = None;
    let mut reported = 0;
    loop {
        let expiry = async {
            match linger {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            document = received.recv() => {
                // Every sender gone: nothing more can arrive.
                let Some(document) = document else { break };
                if !batch.fits(&document) {
                    ship(&*destination, batch.take(), &dropped, &mut warned).await;
                    linger = None;
                }
                linger.get_or_insert_with(|| tokio::time::Instant::now() + BATCH_LINGER);
                batch.push(document);
            }
            _ = expiry => {
                ship(&*destination, batch.take(), &dropped, &mut warned).await;
                linger = None;
            }
            _ = stop.notified() => {
                // What is queued now is the last of it.
                received.close();
                while let Ok(document) = received.try_recv() {
                    if !batch.fits(&document) {
                        ship(&*destination, batch.take(), &dropped, &mut warned).await;
                    }
                    batch.push(document);
                }
                break;
            }
        }
        report(&dropped, &mut reported, &mut warned);
    }
    ship(&*destination, batch.take(), &dropped, &mut warned).await;
    report(&dropped, &mut reported, &mut warned);
}

/// One batch, with the retries [`RETRY_BACKOFF`] allows, then dropped and counted.
async fn ship(
    destination: &dyn TraceDestination,
    documents: Vec<String>,
    dropped: &AtomicU64,
    warned: &mut Option<Instant>,
) {
    if documents.is_empty() {
        return;
    }
    let count = documents.len() as u64;
    let mut tries = RETRY_BACKOFF.iter();
    loop {
        match destination.put(documents.clone()).await {
            Ok(0) => return,
            Ok(refused) => {
                dropped.fetch_add(refused as u64, Ordering::Relaxed);
                if warn_due(warned) {
                    tracing::warn!(refused, "X-Ray refused some guest trace segments");
                }
                return;
            }
            Err(SendError::Definitive(e)) => {
                dropped.fetch_add(count, Ordering::Relaxed);
                if warn_due(warned) {
                    tracing::warn!(
                        error = %e,
                        "X-Ray refused guest traces; the instance role needs xray:PutTraceSegments"
                    );
                }
                return;
            }
            Err(SendError::Transient(e)) => match tries.next() {
                Some(wait) => tokio::time::sleep(*wait).await,
                None => {
                    dropped.fetch_add(count, Ordering::Relaxed);
                    if warn_due(warned) {
                        tracing::warn!(error = %e, "could not reach X-Ray; guest traces dropped");
                    }
                    return;
                }
            },
        }
    }
}

/// Say, at most once per interval, how many traces have been lost so far.
fn report(dropped: &AtomicU64, reported: &mut u64, warned: &mut Option<Instant>) {
    let now = dropped.load(Ordering::Relaxed);
    if now > *reported && warn_due(warned) {
        tracing::warn!(total = now, "guest traces dropped");
        *reported = now;
    }
}

/// The real service, through the SDK.
pub struct XrayDestination {
    client: aws_sdk_xray::Client,
}

impl XrayDestination {
    /// Built the way the guest-log client is (see
    /// [`crate::guest_io::cloudwatch::CloudWatchDestination::connect`]): the default credential
    /// chain through gvproxy to the parent's role, the region given to the chain as well as the
    /// client, the SDK's own retries off in favour of the forwarder's, and short timeouts.
    pub async fn connect(config: &XrayConfig) -> Self {
        use aws_sdk_xray::config::{
            retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion, Region,
        };
        let mut builder = aws_sdk_xray::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .credentials_provider(
                aws_config::default_provider::credentials::DefaultCredentialsChain::builder()
                    .region(Region::new(config.region.clone()))
                    .build()
                    .await,
            )
            .retry_config(RetryConfig::disabled())
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(3))
                    .connect_timeout(Duration::from_secs(3))
                    .build(),
            );
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint);
        }
        XrayDestination {
            client: aws_sdk_xray::Client::from_conf(builder.build()),
        }
    }
}

#[async_trait]
impl TraceDestination for XrayDestination {
    async fn put(&self, documents: Vec<String>) -> Result<usize, SendError> {
        let output = self
            .client
            .put_trace_segments()
            .set_trace_segment_documents(Some(documents))
            .send()
            .await
            .map_err(classify)?;
        Ok(output.unprocessed_trace_segments().len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(key: &str, value: &str) -> Field {
        Field {
            key: key.into(),
            value: value.into(),
        }
    }

    fn parse(document: &str) -> Value {
        serde_json::from_str(document).expect("a segment document is JSON")
    }

    #[test]
    fn spans_nest_by_the_order_they_are_opened() {
        let mut trace = Trace::new("aa".into(), Invocation::Request);
        let outer = trace.open(Level::Info, "outer".into(), vec![]).unwrap();
        let inner = trace
            .open(Level::Debug, "inner".into(), vec![field("k", "v")])
            .unwrap();
        trace.close(inner);
        let sibling = trace.open(Level::Info, "sibling".into(), vec![]).unwrap();
        trace.close(sibling);
        trace.close(outer);

        let doc = parse(&trace.finish(Some("pcr0"), "us-east-1"));
        assert_eq!(doc["name"], SERVICE_NAME);
        assert_eq!(doc["annotations"]["tenant"], "aa");
        assert_eq!(doc["annotations"]["invocation"], "request");
        assert_eq!(doc["metadata"]["enclave"]["image"], "pcr0");
        let outer = &doc["subsegments"][0];
        assert_eq!(outer["name"], "outer");
        assert_eq!(outer["subsegments"][0]["name"], "inner");
        assert_eq!(outer["subsegments"][0]["annotations"]["level"], "debug");
        assert_eq!(
            outer["subsegments"][0]["metadata"]["guest"]["fields"]["k"],
            "v"
        );
        assert_eq!(outer["subsegments"][1]["name"], "sibling");
        assert!(doc["trace_id"].as_str().unwrap().starts_with("1-"));
    }

    #[test]
    fn an_event_lands_on_the_innermost_open_span_or_the_one_it_names() {
        let mut trace = Trace::new("aa".into(), Invocation::Task);
        trace.event(None, Level::Info, "on the root".into(), vec![]);
        let outer = trace.open(Level::Info, "outer".into(), vec![]).unwrap();
        let inner = trace.open(Level::Info, "inner".into(), vec![]).unwrap();
        trace.event(None, Level::Warn, "on inner".into(), vec![]);
        trace.event(
            Some(outer),
            Level::Error,
            "on outer".into(),
            vec![field("a", "b")],
        );
        trace.close(inner);
        trace.close(outer);

        let doc = parse(&trace.finish(None, "r"));
        assert_eq!(
            doc["metadata"]["guest"]["events"][0]["message"],
            "on the root"
        );
        let outer = &doc["subsegments"][0];
        assert_eq!(
            outer["metadata"]["guest"]["events"][0]["message"],
            "on outer"
        );
        assert_eq!(outer["metadata"]["guest"]["events"][0]["level"], "error");
        assert_eq!(
            outer["subsegments"][0]["metadata"]["guest"]["events"][0]["message"],
            "on inner"
        );
    }

    #[test]
    fn closing_twice_is_once_and_finishing_closes_what_was_left_open() {
        let mut trace = Trace::new("aa".into(), Invocation::Message);
        let span = trace.open(Level::Info, "s".into(), vec![]).unwrap();
        assert!(trace.close(span).is_some());
        assert!(trace.close(span).is_none());
        let left = trace.open(Level::Info, "left open".into(), vec![]).unwrap();
        let _ = left;
        let doc = parse(&trace.finish(None, "r"));
        let end = doc["subsegments"][1]["end_time"].as_f64().unwrap();
        let start = doc["subsegments"][1]["start_time"].as_f64().unwrap();
        assert!(end >= start);
        assert!(doc["end_time"].as_f64().unwrap() >= end);
    }

    #[test]
    fn a_full_trace_counts_what_it_could_not_keep() {
        let mut trace = Trace::new("aa".into(), Invocation::Request);
        for i in 0..MAX_SPANS + 10 {
            if let Some(span) = trace.open(Level::Info, format!("s{i}"), vec![]) {
                trace.close(span);
            }
        }
        for _ in 0..MAX_EVENTS + 3 {
            trace.event(None, Level::Info, "e".into(), vec![]);
        }
        let doc = parse(&trace.finish(None, "r"));
        assert!(doc["metadata"]["guest"]["dropped_spans"].as_u64().unwrap() > 0);
        assert_eq!(doc["metadata"]["guest"]["dropped_events"], 3);
    }

    #[test]
    fn a_trace_too_big_for_x_ray_sheds_detail_rather_than_being_refused() {
        let mut trace = Trace::new("aa".into(), Invocation::Request);
        for _ in 0..200 {
            let span = trace.open(Level::Info, "s".into(), vec![]).unwrap();
            for _ in 0..MAX_EVENTS {
                trace.event(Some(span), Level::Info, "x".repeat(MAX_TEXT_CHARS), vec![]);
            }
            trace.close(span);
        }
        let document = trace.finish(None, "r");
        assert!(
            document.len() <= MAX_DOCUMENT_BYTES,
            "{} bytes",
            document.len()
        );
        assert_eq!(parse(&document)["metadata"]["guest"]["trimmed"], true);
    }

    #[test]
    fn guest_text_is_clipped_and_span_names_are_made_acceptable() {
        let mut trace = Trace::new("aa".into(), Invocation::Request);
        let span = trace
            .open(
                Level::Info,
                "bad<name>{}".into(),
                vec![field(&"k".repeat(200), "v")],
            )
            .unwrap();
        trace.set_field(span, "x".into(), "é".repeat(MAX_TEXT_CHARS + 5));
        trace.close(span);
        let empty = trace.open(Level::Info, "   ".into(), vec![]).unwrap();
        trace.close(empty);
        let doc = parse(&trace.finish(None, "r"));
        assert_eq!(doc["subsegments"][0]["name"], "bad_name___");
        assert_eq!(doc["subsegments"][1]["name"], "span");
        let fields = doc["subsegments"][0]["metadata"]["guest"]["fields"]
            .as_object()
            .unwrap();
        assert!(fields.keys().all(|k| k.chars().count() <= MAX_KEY_CHARS));
        assert_eq!(
            fields["x"].as_str().unwrap().chars().count(),
            MAX_TEXT_CHARS
        );
    }

    /// Records what it is given; fails transiently `fail` times first.
    struct Recorder {
        calls: Mutex<Vec<Vec<String>>>,
        fail: AtomicU64,
        definitive: bool,
    }

    impl Recorder {
        fn new(fail: u64, definitive: bool) -> Arc<Self> {
            Arc::new(Recorder {
                calls: Mutex::new(Vec::new()),
                fail: AtomicU64::new(fail),
                definitive,
            })
        }
        fn documents(&self) -> Vec<String> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .cloned()
                .collect()
        }
    }

    #[async_trait]
    impl TraceDestination for Recorder {
        async fn put(&self, documents: Vec<String>) -> Result<usize, SendError> {
            self.calls.lock().unwrap().push(documents);
            if self.fail.load(Ordering::SeqCst) > 0 {
                self.fail.fetch_sub(1, Ordering::SeqCst);
                return Err(if self.definitive {
                    SendError::Definitive("no".into())
                } else {
                    SendError::Transient("later".into())
                });
            }
            Ok(0)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_trace_is_shipped_after_the_linger_and_everything_queued_goes_at_shutdown() {
        let recorder = Recorder::new(0, false);
        let tracer = Tracer::start(recorder.clone(), None, "r".into());
        tracer.submit("one".into());
        tokio::time::sleep(BATCH_LINGER * 2).await;
        assert_eq!(recorder.documents(), vec!["one".to_string()]);

        tracer.submit("two".into());
        tracer.submit("three".into());
        tracer.shutdown().await;
        assert_eq!(recorder.documents(), ["one", "two", "three"]);
        assert_eq!(tracer.dropped(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_transient_failure_is_retried_and_a_definitive_one_is_dropped() {
        let flaky = Recorder::new(1, false);
        let tracer = Tracer::start(flaky.clone(), None, "r".into());
        tracer.submit("d".into());
        tracer.shutdown().await;
        assert_eq!(flaky.calls.lock().unwrap().len(), 2, "retried once");
        assert_eq!(tracer.dropped(), 0);

        let refusing = Recorder::new(u64::MAX, true);
        let tracer = Tracer::start(refusing.clone(), None, "r".into());
        tracer.submit("d".into());
        tracer.shutdown().await;
        assert_eq!(refusing.calls.lock().unwrap().len(), 1, "never retried");
        assert_eq!(tracer.dropped(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_batch_never_exceeds_the_document_limit() {
        let recorder = Recorder::new(0, false);
        let tracer = Tracer::start(recorder.clone(), None, "r".into());
        for i in 0..MAX_BATCH_DOCUMENTS + 5 {
            tracer.submit(format!("{i}"));
        }
        tracer.shutdown().await;
        let calls = recorder.calls.lock().unwrap();
        assert!(calls.iter().all(|c| c.len() <= MAX_BATCH_DOCUMENTS));
        assert_eq!(
            calls.iter().map(Vec::len).sum::<usize>(),
            MAX_BATCH_DOCUMENTS + 5
        );
    }
}
