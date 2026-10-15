#!/usr/bin/env bash
# Put the released cosigner at target/cosigner/cosigner.wasm.
#
# The one guest this runtime is built to serve, as its own repository released
# it: the release and sha256 are pinned in cosigner.lock. CI serves it in
# process (runtime/tests/serve_cosigner.rs) and boots it under QEMU (run-e2e.sh
# with GUEST_AGNOSTIC=1), so a runtime change that breaks it fails here rather
# than after its release.
#
# COSIGNER_WASM names a local build to use instead, for a change that needs
# both sides at once. Idempotent otherwise: a file that already matches the pin
# is kept.

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
# shellcheck source=../cosigner.lock
. "$REPO/cosigner.lock"

out="$REPO/target/cosigner/cosigner.wasm"
mkdir -p "$(dirname "$out")"

if [[ -n "${COSIGNER_WASM:-}" ]]; then
    install -m 0644 "$COSIGNER_WASM" "$out"
    echo "cosigner from $COSIGNER_WASM, not $RELEASE"
    exit 0
fi

if echo "$SHA256  $out" | sha256sum -c --status - 2>/dev/null; then
    echo "cosigner $RELEASE already at $out"
    exit 0
fi

say "fetching cosigner $RELEASE"
url="https://github.com/vtxos/cosigner/releases/download/$RELEASE/cosigner.wasm"
curl -fL --retry 3 --retry-all-errors --max-time 300 -o "$out.part" "$url" \
    || fail "could not download $url"
echo "$SHA256  $out.part" | sha256sum -c - \
    || fail "cosigner $RELEASE does not match cosigner.lock"
mv "$out.part" "$out"
echo "cosigner $RELEASE at $out"
