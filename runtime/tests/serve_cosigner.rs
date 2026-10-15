//! The cosigner, as released, served by this runtime.
//!
//! The one guest this runtime is built to serve lives in its own repository and
//! is pinned here by release (`cosigner.lock`). This is the check that a runtime
//! change still serves it: every import it makes resolves, it instantiates,
//! opens its tenant's store, and answers a gRPC call through the gate. What its
//! ceremonies do is the cosigner's own suites' to prove.
//!
//! ```console
//! $ scripts/cosigner.sh
//! $ cargo test -p enclave-runtime --test serve_cosigner -- --include-ignored
//! ```

use enclave_runtime::testing::Enclave;

fn cosigner() -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../target/cosigner/cosigner.wasm");
    std::fs::read(path).expect("fetch the released cosigner first: scripts/cosigner.sh")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs the released cosigner: scripts/cosigner.sh"]
async fn the_released_cosigner_links_and_answers_through_the_gate() {
    // Tasks and notify are what its world imports beyond WASI and telemetry,
    // which is always linked; a runtime that stopped providing any of them
    // fails here, at instantiation.
    let enclave = Enclave::builder(cosigner())
        .background_tasks()
        .notify()
        .start()
        .await
        .unwrap();
    let alice = enclave.enrol().await.unwrap();

    // GetServerInfo, unary: one empty message as a gRPC frame, uncompressed
    // and zero bytes long. The answer is a frame carrying the network the
    // cosigner was configured for, regtest when nothing says otherwise.
    let (status, body) = enclave
        .signed(
            &alice,
            "POST",
            "/cosigner.v1.Cosigner/GetServerInfo",
            "\0\0\0\0\0",
        )
        .await
        .unwrap();
    assert_eq!(status, 200, "{body:?}");
    assert!(
        body.contains("regtest"),
        "the cosigner did not answer with its network: {body:?}"
    );
}
