//! The store-data type both binaries hand to wasmtime.

use wasmtime::component::ResourceTable;
use wasmtime::{Result, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

/// Implements `WasiView`, so `wasmtime-wasi` serves every WASI interface —
/// the filesystem included, over the directory the guest was preopened.
pub struct State {
    pub(crate) tasks: Option<crate::tasks::TaskContext>,
    /// The connections the runtime holds for this tenant — see [`crate::stream`].
    pub(crate) streams: Option<crate::stream::StreamContext>,
    pub(crate) notify: Option<crate::notify::NotifyContext>,
    /// This invocation's trace and where its metrics go — see [`crate::telemetry`]. Its trace is
    /// shipped when this is dropped, which is when the store is.
    pub(crate) telemetry: Option<crate::telemetry::TelemetryContext>,
    wasi: WasiCtx,
    table: ResourceTable,
    http: wasmtime_wasi_http::WasiHttpCtx,
    /// What `wasi:http/outgoing-handler` reaches: the public internet, and the
    /// same for every guest — see [`crate::serve::GuestEgress`].
    egress: crate::serve::GuestEgress,
}

impl State {
    pub fn new(wasi: WasiCtx) -> Self {
        State {
            tasks: None,
            streams: None,
            notify: None,
            telemetry: None,
            wasi,
            table: ResourceTable::new(),
            http: wasmtime_wasi_http::WasiHttpCtx::new(),
            egress: crate::serve::GuestEgress,
        }
    }

    /// The telemetry context and the resource table at once. A guest's span is an entry in the
    /// table that points into the context's trace, so every span call needs both halves.
    pub(crate) fn telemetry_and_table(
        &mut self,
    ) -> (
        Option<&mut crate::telemetry::TelemetryContext>,
        &mut ResourceTable,
    ) {
        (self.telemetry.as_mut(), &mut self.table)
    }
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

/// `wasi:http` needs its own context and its own hooks, projected out of the
/// same `ResourceTable` as everything else — a second table would hand the
/// guest stream handles that look valid and resolve to nothing.
impl wasmtime_wasi_http::p2::WasiHttpView for State {
    fn http(&mut self) -> wasmtime_wasi_http::p2::WasiHttpCtxView<'_> {
        wasmtime_wasi_http::p2::WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.egress,
        }
    }
}

/// Convenience for callers that need a `Store` without naming `State`'s
/// internals.
pub fn new_store(engine: &wasmtime::Engine, state: State) -> Result<Store<State>> {
    Ok(Store::new(engine, state))
}
