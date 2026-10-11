//! Guest telemetry, end to end.
//!
//! A real component calls `enclave:telemetry` through the runtime's own linker — so the
//! hand-wired names and types in `telemetry::add_to_linker` are checked against what wit-bindgen
//! actually generates — and what would reach X-Ray and CloudWatch is inspected. Both services are
//! replaced by in-memory destinations, so nothing here needs AWS.
//!
//! ```console
//! $ scripts/build-guest.sh http
//! $ cargo test -p enclave-runtime --test telemetry -- --ignored
//! ```

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use enclave_runtime::telemetry::{
    Datum, DatumValue, MetricDestination, Metrics, SendError, Telemetry, TraceDestination, Tracer,
    Unit,
};
use enclave_runtime::testing::Enclave;
use serde_json::Value;

fn guest() -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../examples/guest-http/target/wasm32-wasip2/release/guest-http.wasm");
    std::fs::read(path).expect("build examples/guest-http for wasm32-wasip2")
}

#[derive(Default)]
struct Traces(Mutex<Vec<String>>);

#[async_trait]
impl TraceDestination for Traces {
    async fn put(&self, documents: Vec<String>) -> Result<usize, SendError> {
        self.0.lock().unwrap().extend(documents);
        Ok(0)
    }
}

#[derive(Default)]
struct Data(Mutex<Vec<(String, Datum)>>);

#[async_trait]
impl MetricDestination for Data {
    async fn put(&self, namespace: &str, data: Vec<Datum>, _: SystemTime) -> Result<(), SendError> {
        let mut seen = self.0.lock().unwrap();
        seen.extend(data.into_iter().map(|d| (namespace.to_string(), d)));
        Ok(())
    }
}

/// The trace whose invocation opened a span named `name`, once it has been shipped. A trace goes
/// when its invocation's store is dropped, a little after the response, and then waits out the
/// forwarder's linger — so this polls rather than assuming.
async fn trace_with_span(traces: &Traces, name: &str) -> Value {
    for _ in 0..100 {
        let found = traces
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|d| serde_json::from_str::<Value>(d).expect("a segment document is JSON"))
            .find(|d| d["subsegments"][0]["name"] == name);
        if let Some(trace) = found {
            return trace;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no trace with a `{name}` span arrived");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs examples/guest-http built for wasm32-wasip2"]
async fn a_guests_spans_and_metrics_reach_x_ray_and_cloudwatch_attributed_to_its_tenant() {
    let traces = Arc::new(Traces::default());
    let data = Arc::new(Data::default());
    let tracer = Tracer::start(
        traces.clone(),
        Some("pcr0-of-this-image".into()),
        "us-east-1".into(),
    );
    let metrics = Metrics::start(data.clone(), "Test/Guest".into());
    let enclave = Enclave::builder(guest())
        .telemetry(Telemetry::new(Some(tracer.clone()), Some(metrics.clone())))
        .start()
        .await
        .unwrap();

    let alice = enclave.enrol().await.unwrap();
    let (status, body) = enclave
        .signed(&alice, "GET", "/telemetry", "")
        .await
        .unwrap();
    assert_eq!(status, 200, "{body}");

    // --- The trace -----------------------------------------------------------------------------
    let trace = trace_with_span(&traces, "handle").await;
    assert_eq!(
        trace["annotations"]["tenant"],
        alice.tenant(),
        "attributed to its tenant"
    );
    assert_eq!(trace["annotations"]["invocation"], "request");
    assert_eq!(trace["metadata"]["enclave"]["image"], "pcr0-of-this-image");
    assert_eq!(trace["metadata"]["enclave"]["region"], "us-east-1");

    let handle = &trace["subsegments"][0];
    assert_eq!(handle["annotations"]["level"], "info");
    assert_eq!(handle["metadata"]["guest"]["fields"]["route"], "/telemetry");
    assert_eq!(
        handle["metadata"]["guest"]["fields"]["phase"], "start",
        "set-field"
    );
    assert_eq!(
        handle["metadata"]["guest"]["events"][0]["message"], "began",
        "add-event"
    );

    let inner = &handle["subsegments"][0];
    assert_eq!(
        inner["name"], "inner",
        "opened inside `handle`, so its child"
    );
    assert_eq!(inner["annotations"]["level"], "debug");
    let event = &inner["metadata"]["guest"]["events"][0];
    assert_eq!(
        event["message"], "inside inner",
        "a free event lands on the innermost span"
    );
    assert_eq!(event["level"], "warn");
    assert_eq!(event["fields"]["n"], "1");

    // `inner` was closed by dropping its handle, `handle` by `end`; both have a real duration.
    for span in [handle, inner] {
        assert!(span["end_time"].as_f64().unwrap() >= span["start_time"].as_f64().unwrap());
    }
    assert!(
        inner["end_time"].as_f64().unwrap() <= handle["end_time"].as_f64().unwrap(),
        "the child closed before its parent"
    );

    // --- The metrics ---------------------------------------------------------------------------
    // Aggregated in memory until a flush; shutdown is one.
    metrics.shutdown().await;
    let data = data.0.lock().unwrap().clone();
    let find = |name: &str| {
        data.iter()
            .find(|(_, d)| d.name == name)
            .unwrap_or_else(|| panic!("no `{name}` datum in {data:?}"))
    };
    let route = vec![("route".to_string(), "telemetry".to_string())];

    let (namespace, requests) = find("requests");
    assert_eq!(namespace, "Test/Guest");
    assert_eq!(requests.value, DatumValue::Single(1.0));
    assert_eq!(requests.unit, Unit::Count);
    assert_eq!(requests.dimensions, route);

    let (_, depth) = find("queue-depth");
    assert_eq!(depth.value, DatumValue::Single(3.0));
    assert!(depth.dimensions.is_empty(), "no tenant dimension is added");

    let (_, latency) = find("latency");
    assert_eq!(latency.unit, Unit::Milliseconds);
    assert_eq!(
        latency.value,
        DatumValue::Spread {
            values: vec![12.5],
            counts: vec![1.0]
        }
    );

    tracer.shutdown().await;
    assert_eq!(tracer.dropped(), 0);
    assert_eq!(metrics.dropped(), 0);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs examples/guest-http built for wasm32-wasip2"]
async fn with_telemetry_off_a_guests_calls_still_succeed() {
    // The default: no X-Ray, no metrics. The guest is the same one, and it must not fail.
    let enclave = Enclave::builder(guest()).start().await.unwrap();
    let alice = enclave.enrol().await.unwrap();
    let (status, body) = enclave
        .signed(&alice, "GET", "/telemetry", "")
        .await
        .unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(body.trim(), "reported");
}
