#!/usr/bin/env bash
# Put the cosigner release cosigner.lock pins at target/cosigner/cosigner.wasm,
# refusing any other bytes. ci-e2e.sh boots it as the e2e's second guest.

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
# shellcheck source=../cosigner.lock
. "$REPO/cosigner.lock"

out="$REPO/target/cosigner/cosigner.wasm"
if echo "$SHA256  $out" | sha256sum -c --status - 2>/dev/null; then
    echo "cosigner $RELEASE already at $out"
    exit 0
fi

say "fetching cosigner $RELEASE"
mkdir -p "$(dirname "$out")"
url="https://github.com/vtxos/cosigner/releases/download/$RELEASE/cosigner.wasm"
curl -fL --retry 3 --retry-all-errors --max-time 300 -o "$out.part" "$url" \
    || fail "could not download $url"
echo "$SHA256  $out.part" | sha256sum -c - \
    || fail "cosigner $RELEASE does not match cosigner.lock"
mv "$out.part" "$out"
