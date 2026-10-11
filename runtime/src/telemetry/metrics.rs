//! Guest metrics, aggregated in memory and published to CloudWatch with `PutMetricData`.
//!
//! Aggregated, not queued: a counter a guest bumps a thousand times between flushes is one datum,
//! and CloudWatch bills per request and per datum. Counters sum, gauges keep their last value, and
//! histograms keep each distinct value with its count — CloudWatch's own `values`/`counts`
//! distribution, so percentiles survive aggregation.
//!
//! A metric is checked when it is recorded, not when it is sent. CloudWatch refuses a whole
//! request over one bad datum — a value out of its range, a repeated dimension — so letting one
//! through would take every other guest metric in the same call with it. A refused metric is
//! dropped and counted, as the WIT promises.
//!
//! Series are capped per window, which bounds memory and bills alike: a guest that mints a new
//! dimension value per request gets its first [`MAX_SERIES`] and a count of the rest.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use tokio::sync::Notify;

use super::{classify, warn_due, Dimension, SendError, Unit, FLUSH_DEADLINE, RETRY_BACKOFF};

/// How often aggregated metrics go to CloudWatch. Its standard resolution is a minute.
const FLUSH_INTERVAL: Duration = Duration::from_secs(30);
/// Distinct series (name, kind, unit and dimensions) per window.
const MAX_SERIES: usize = 1000;
/// Datums per `PutMetricData` call.
const MAX_DATUMS_PER_CALL: usize = 1000;
/// Distinct values in one histogram datum — CloudWatch's limit on `values`.
const MAX_DISTINCT_VALUES: usize = 150;
/// Dimensions per metric, as the WIT says.
const MAX_DIMENSIONS: usize = 10;
/// The magnitudes CloudWatch accepts; one value outside them fails the whole request.
const MAX_MAGNITUDE: f64 = 8.515920e+109;
const MIN_MAGNITUDE: f64 = 8.515920e-109;

/// Where the CloudWatch client sends, and under which namespace.
#[derive(Clone, Debug)]
pub struct MetricsConfig {
    pub namespace: String,
    pub region: String,
    /// A stand-in for the service, for an emulator. `None` is the real one.
    pub endpoint: Option<String>,
}

impl MetricsConfig {
    /// A namespace CloudWatch accepts for custom metrics: 1-255 printable ASCII characters, and
    /// not under `AWS/`, which is reserved for AWS's own.
    pub fn valid_namespace(namespace: &str) -> bool {
        (1..=255).contains(&namespace.len())
            && namespace.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
            && !namespace.starts_with("AWS/")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Kind {
    Counter,
    Gauge,
    Histogram,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Series {
    name: String,
    kind: Kind,
    unit: Unit,
    /// Sorted by name, so the same dimensions in any order are one series.
    dimensions: Vec<(String, String)>,
}

enum Aggregate {
    Sum(f64),
    Last(f64),
    Spread(Vec<(f64, f64)>),
}

/// One CloudWatch datum, before it becomes the SDK's type. Public as the destination's seam.
#[derive(Clone, Debug, PartialEq)]
pub struct Datum {
    pub name: String,
    pub unit: Unit,
    pub dimensions: Vec<(String, String)>,
    pub value: DatumValue,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DatumValue {
    Single(f64),
    Spread { values: Vec<f64>, counts: Vec<f64> },
}

fn datum(series: &Series, aggregate: Aggregate) -> Datum {
    Datum {
        name: series.name.clone(),
        unit: series.unit,
        dimensions: series.dimensions.clone(),
        value: match aggregate {
            Aggregate::Sum(v) | Aggregate::Last(v) => DatumValue::Single(v),
            Aggregate::Spread(points) => {
                let (values, counts) = points.into_iter().unzip();
                DatumValue::Spread { values, counts }
            }
        },
    }
}

#[derive(Default)]
struct Window {
    series: HashMap<Series, Aggregate>,
    /// Histogram datums sealed early because they hit [`MAX_DISTINCT_VALUES`].
    sealed: Vec<Datum>,
}

impl Window {
    /// Add one observation. `false` when the series is new and the window already holds
    /// [`MAX_SERIES`].
    fn add(&mut self, series: Series, value: f64) -> bool {
        if self.series.len() >= MAX_SERIES && !self.series.contains_key(&series) {
            return false;
        }
        match self.series.entry(series) {
            Entry::Vacant(entry) => {
                let aggregate = match entry.key().kind {
                    Kind::Counter => Aggregate::Sum(value),
                    Kind::Gauge => Aggregate::Last(value),
                    Kind::Histogram => Aggregate::Spread(vec![(value, 1.0)]),
                };
                entry.insert(aggregate);
            }
            Entry::Occupied(mut entry) => {
                let full = match entry.get_mut() {
                    Aggregate::Sum(sum) => {
                        *sum += value;
                        None
                    }
                    Aggregate::Last(last) => {
                        *last = value;
                        None
                    }
                    Aggregate::Spread(points) => {
                        if let Some(point) = points.iter_mut().find(|p| p.0 == value) {
                            point.1 += 1.0;
                            None
                        } else if points.len() < MAX_DISTINCT_VALUES {
                            points.push((value, 1.0));
                            None
                        } else {
                            Some(std::mem::replace(points, vec![(value, 1.0)]))
                        }
                    }
                };
                if let Some(points) = full {
                    self.sealed
                        .push(datum(entry.key(), Aggregate::Spread(points)));
                }
            }
        }
        true
    }

    fn take(&mut self) -> Vec<Datum> {
        let mut data = std::mem::take(&mut self.sealed);
        data.extend(
            self.series
                .drain()
                .map(|(series, aggregate)| datum(&series, aggregate)),
        );
        data
    }
}

/// Where flushed metrics go. A trait so aggregation and retries can be tested without HTTP.
#[async_trait]
pub trait MetricDestination: Send + Sync {
    async fn put(
        &self,
        namespace: &str,
        data: Vec<Datum>,
        timestamp: SystemTime,
    ) -> Result<(), SendError>;
}

struct Shared {
    window: Mutex<Window>,
    /// Metrics refused when recorded, past the series cap, or lost in delivery.
    dropped: AtomicU64,
}

impl Shared {
    fn window(&self) -> std::sync::MutexGuard<'_, Window> {
        self.window
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Aggregates guest metrics and publishes them, on a task of its own.
pub struct Metrics {
    shared: Arc<Shared>,
    stop: Arc<Notify>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Metrics {
    /// Start publishing to `destination` under `namespace`.
    pub fn start(destination: Arc<dyn MetricDestination>, namespace: String) -> Arc<Self> {
        let shared = Arc::new(Shared {
            window: Mutex::new(Window::default()),
            dropped: AtomicU64::new(0),
        });
        let stop = Arc::new(Notify::new());
        let task = tokio::spawn(publish_loop(
            destination,
            namespace,
            shared.clone(),
            stop.clone(),
        ));
        Arc::new(Metrics {
            shared,
            stop,
            task: Mutex::new(Some(task)),
        })
    }

    /// Record one observation, or drop and count it if CloudWatch would refuse it.
    pub(super) fn record(
        &self,
        kind: Kind,
        name: String,
        value: f64,
        unit: Unit,
        dimensions: Vec<Dimension>,
    ) {
        let added = match series(kind, name, unit, dimensions, value) {
            Some(series) => self.shared.window().add(series, value),
            None => false,
        };
        if !added {
            self.shared.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Metrics dropped so far, refused or lost.
    pub fn dropped(&self) -> u64 {
        self.shared.dropped.load(Ordering::Relaxed)
    }

    /// Publish what has been aggregated and stop, within [`FLUSH_DEADLINE`].
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

/// The series an observation belongs to, or `None` if CloudWatch would refuse it.
fn series(
    kind: Kind,
    name: String,
    unit: Unit,
    dimensions: Vec<Dimension>,
    value: f64,
) -> Option<Series> {
    if !valid_metric_name(&name) || !in_range(value) || dimensions.len() > MAX_DIMENSIONS {
        return None;
    }
    let mut dimensions: Vec<(String, String)> =
        dimensions.into_iter().map(|d| (d.name, d.value)).collect();
    if !dimensions
        .iter()
        .all(|(n, v)| valid_dimension_part(n) && valid_dimension_part(v))
    {
        return None;
    }
    dimensions.sort();
    // CloudWatch refuses a datum that names one dimension twice.
    if dimensions.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return None;
    }
    Some(Series {
        name,
        kind,
        unit,
        dimensions,
    })
}

/// 1-255 printable ASCII characters, not all spaces.
fn valid_metric_name(name: &str) -> bool {
    (1..=255).contains(&name.len())
        && name.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
        && name.bytes().any(|b| b != b' ')
}

/// What the WIT promises for a dimension's name and value: 1-64 of `[A-Za-z0-9_.-]`.
fn valid_dimension_part(part: &str) -> bool {
    (1..=64).contains(&part.len())
        && part
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn in_range(value: f64) -> bool {
    value.is_finite() && (value == 0.0 || (MIN_MAGNITUDE..=MAX_MAGNITUDE).contains(&value.abs()))
}

async fn publish_loop(
    destination: Arc<dyn MetricDestination>,
    namespace: String,
    shared: Arc<Shared>,
    stop: Arc<Notify>,
) {
    let mut tick = tokio::time::interval(FLUSH_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The first tick is immediate, and there is nothing to send yet.
    tick.tick().await;
    let mut warned = None;
    let mut reported = 0;
    loop {
        let last = tokio::select! {
            _ = tick.tick() => false,
            _ = stop.notified() => true,
        };
        let data = shared.window().take();
        publish(
            &*destination,
            &namespace,
            data,
            &shared.dropped,
            &mut warned,
        )
        .await;
        let dropped = shared.dropped.load(Ordering::Relaxed);
        if dropped > reported && warn_due(&mut warned) {
            tracing::warn!(
                total = dropped,
                "guest metrics dropped: refused as CloudWatch would refuse them, past the series \
                 cap, or not delivered"
            );
            reported = dropped;
        }
        if last {
            return;
        }
    }
}

async fn publish(
    destination: &dyn MetricDestination,
    namespace: &str,
    data: Vec<Datum>,
    dropped: &AtomicU64,
    warned: &mut Option<Instant>,
) {
    let timestamp = SystemTime::now();
    for chunk in data.chunks(MAX_DATUMS_PER_CALL) {
        let mut tries = RETRY_BACKOFF.iter();
        loop {
            match destination.put(namespace, chunk.to_vec(), timestamp).await {
                Ok(()) => break,
                Err(SendError::Definitive(e)) => {
                    dropped.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    if warn_due(warned) {
                        tracing::warn!(
                            error = %e,
                            "CloudWatch refused guest metrics; the instance role needs \
                             cloudwatch:PutMetricData"
                        );
                    }
                    break;
                }
                Err(SendError::Transient(e)) => match tries.next() {
                    Some(wait) => tokio::time::sleep(*wait).await,
                    None => {
                        dropped.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                        if warn_due(warned) {
                            tracing::warn!(error = %e, "could not reach CloudWatch; guest metrics dropped");
                        }
                        break;
                    }
                },
            }
        }
    }
}

/// The real service, through the SDK.
pub struct CloudWatchMetrics {
    client: aws_sdk_cloudwatch::Client,
}

impl CloudWatchMetrics {
    /// Built as [`super::XrayDestination::connect`] is, and for the same reasons.
    pub async fn connect(config: &MetricsConfig) -> Self {
        use aws_sdk_cloudwatch::config::{
            retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion, Region,
        };
        let mut builder = aws_sdk_cloudwatch::Config::builder()
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
        CloudWatchMetrics {
            client: aws_sdk_cloudwatch::Client::from_conf(builder.build()),
        }
    }
}

#[async_trait]
impl MetricDestination for CloudWatchMetrics {
    async fn put(
        &self,
        namespace: &str,
        data: Vec<Datum>,
        timestamp: SystemTime,
    ) -> Result<(), SendError> {
        use aws_sdk_cloudwatch::primitives::DateTime;
        use aws_sdk_cloudwatch::types::{Dimension, MetricDatum, StandardUnit};
        let at = DateTime::from(timestamp);
        let data = data
            .into_iter()
            .map(|d| {
                let unit = match d.unit {
                    Unit::None => StandardUnit::None,
                    Unit::Count => StandardUnit::Count,
                    Unit::Seconds => StandardUnit::Seconds,
                    Unit::Milliseconds => StandardUnit::Milliseconds,
                    Unit::Bytes => StandardUnit::Bytes,
                    Unit::Kilobytes => StandardUnit::Kilobytes,
                    Unit::Percent => StandardUnit::Percent,
                };
                let mut datum = MetricDatum::builder()
                    .metric_name(d.name)
                    .unit(unit)
                    .timestamp(at);
                for (name, value) in d.dimensions {
                    datum = datum.dimensions(Dimension::builder().name(name).value(value).build());
                }
                match d.value {
                    DatumValue::Single(value) => datum.value(value),
                    DatumValue::Spread { values, counts } => {
                        datum.set_values(Some(values)).set_counts(Some(counts))
                    }
                }
                .build()
            })
            .collect();
        self.client
            .put_metric_data()
            .namespace(namespace)
            .set_metric_data(Some(data))
            .send()
            .await
            .map_err(classify)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dim(name: &str, value: &str) -> Dimension {
        Dimension {
            name: name.into(),
            value: value.into(),
        }
    }

    fn take(metrics: &Metrics) -> Vec<Datum> {
        let mut data = metrics.shared.window().take();
        data.sort_by(|a, b| a.name.cmp(&b.name));
        data
    }

    /// Records every call; fails transiently `fail` times first.
    struct Recorder {
        calls: Mutex<Vec<(String, Vec<Datum>)>>,
        fail: AtomicU64,
    }

    #[async_trait]
    impl MetricDestination for Recorder {
        async fn put(
            &self,
            namespace: &str,
            data: Vec<Datum>,
            _: SystemTime,
        ) -> Result<(), SendError> {
            self.calls
                .lock()
                .unwrap()
                .push((namespace.to_string(), data));
            if self.fail.load(Ordering::SeqCst) > 0 {
                self.fail.fetch_sub(1, Ordering::SeqCst);
                return Err(SendError::Transient("later".into()));
            }
            Ok(())
        }
    }

    fn recorder(fail: u64) -> Arc<Recorder> {
        Arc::new(Recorder {
            calls: Mutex::new(Vec::new()),
            fail: AtomicU64::new(fail),
        })
    }

    #[tokio::test]
    async fn counters_sum_gauges_keep_the_last_value_and_histograms_count_each_value() {
        let metrics = Metrics::start(recorder(0), "ns".into());
        metrics.record(Kind::Counter, "c".into(), 2.0, Unit::Count, vec![]);
        metrics.record(Kind::Counter, "c".into(), 3.0, Unit::Count, vec![]);
        metrics.record(Kind::Gauge, "g".into(), 1.0, Unit::Percent, vec![]);
        metrics.record(Kind::Gauge, "g".into(), 7.0, Unit::Percent, vec![]);
        for v in [5.0, 5.0, 9.0] {
            metrics.record(Kind::Histogram, "h".into(), v, Unit::Milliseconds, vec![]);
        }
        let data = take(&metrics);
        assert_eq!(data[0].value, DatumValue::Single(5.0));
        assert_eq!(data[1].value, DatumValue::Single(7.0));
        assert_eq!(data[1].unit, Unit::Percent);
        let DatumValue::Spread { values, counts } = &data[2].value else {
            panic!("a histogram is a distribution")
        };
        let mut points: Vec<(f64, f64)> =
            values.iter().copied().zip(counts.iter().copied()).collect();
        points.sort_by(|a, b| a.0.total_cmp(&b.0));
        assert_eq!(points, [(5.0, 2.0), (9.0, 1.0)]);
        assert_eq!(metrics.dropped(), 0);
    }

    #[tokio::test]
    async fn the_same_dimensions_in_any_order_are_one_series() {
        let metrics = Metrics::start(recorder(0), "ns".into());
        metrics.record(
            Kind::Counter,
            "c".into(),
            1.0,
            Unit::Count,
            vec![dim("a", "1"), dim("b", "2")],
        );
        metrics.record(
            Kind::Counter,
            "c".into(),
            1.0,
            Unit::Count,
            vec![dim("b", "2"), dim("a", "1")],
        );
        let data = take(&metrics);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].value, DatumValue::Single(2.0));
    }

    #[tokio::test]
    async fn what_cloudwatch_would_refuse_is_dropped_and_counted() {
        let metrics = Metrics::start(recorder(0), "ns".into());
        let refused: Vec<(String, f64, Vec<Dimension>)> = vec![
            ("".into(), 1.0, vec![]),
            ("ok".into(), f64::NAN, vec![]),
            ("ok".into(), f64::INFINITY, vec![]),
            ("ok".into(), 1e200, vec![]),
            ("ok".into(), 1e-200, vec![]),
            ("ok".into(), 1.0, vec![dim("a", "1"), dim("a", "2")]),
            ("ok".into(), 1.0, vec![dim("has space", "1")]),
            (
                "ok".into(),
                1.0,
                (0..11).map(|i| dim(&format!("d{i}"), "v")).collect(),
            ),
        ];
        let count = refused.len() as u64;
        for (name, value, dimensions) in refused {
            metrics.record(Kind::Gauge, name, value, Unit::None, dimensions);
        }
        metrics.record(Kind::Gauge, "zero is fine".into(), 0.0, Unit::None, vec![]);
        assert_eq!(metrics.dropped(), count);
        assert_eq!(take(&metrics).len(), 1);
    }

    #[tokio::test]
    async fn a_histogram_past_the_distinct_value_limit_is_sealed_not_lost() {
        let metrics = Metrics::start(recorder(0), "ns".into());
        for v in 0..(MAX_DISTINCT_VALUES + 10) {
            metrics.record(Kind::Histogram, "h".into(), v as f64, Unit::None, vec![]);
        }
        let data = take(&metrics);
        assert_eq!(data.len(), 2);
        let observations: usize = data
            .iter()
            .map(|d| match &d.value {
                DatumValue::Spread { values, .. } => values.len(),
                DatumValue::Single(_) => 0,
            })
            .sum();
        assert_eq!(observations, MAX_DISTINCT_VALUES + 10);
    }

    #[tokio::test]
    async fn new_series_past_the_cap_are_dropped_while_existing_ones_still_count() {
        let metrics = Metrics::start(recorder(0), "ns".into());
        for i in 0..MAX_SERIES {
            metrics.record(Kind::Counter, format!("c{i}"), 1.0, Unit::Count, vec![]);
        }
        metrics.record(
            Kind::Counter,
            "one too many".into(),
            1.0,
            Unit::Count,
            vec![],
        );
        metrics.record(Kind::Counter, "c0".into(), 1.0, Unit::Count, vec![]);
        assert_eq!(metrics.dropped(), 1);
        let data = take(&metrics);
        assert_eq!(data.len(), MAX_SERIES);
    }

    #[tokio::test(start_paused = true)]
    async fn metrics_are_published_on_the_interval_and_at_shutdown_with_a_retry() {
        let destination = recorder(1);
        let metrics = Metrics::start(destination.clone(), "Merlin/Guest".into());
        metrics.record(Kind::Counter, "c".into(), 1.0, Unit::Count, vec![]);
        tokio::time::sleep(FLUSH_INTERVAL + Duration::from_secs(5)).await;
        {
            let calls = destination.calls.lock().unwrap();
            assert_eq!(calls.len(), 2, "one transient failure, then the retry");
            assert_eq!(calls[1].0, "Merlin/Guest");
            assert_eq!(calls[1].1.len(), 1);
        }
        metrics.record(Kind::Counter, "c".into(), 4.0, Unit::Count, vec![]);
        metrics.shutdown().await;
        let calls = destination.calls.lock().unwrap();
        assert_eq!(calls.last().unwrap().1[0].value, DatumValue::Single(4.0));
        assert_eq!(metrics.dropped(), 0);
    }

    #[test]
    fn namespaces_cloudwatch_reserves_or_refuses_are_rejected() {
        assert!(MetricsConfig::valid_namespace("Merlin/Guest"));
        assert!(!MetricsConfig::valid_namespace(""));
        assert!(!MetricsConfig::valid_namespace("AWS/EC2"));
        assert!(!MetricsConfig::valid_namespace("tab\there"));
    }
}
