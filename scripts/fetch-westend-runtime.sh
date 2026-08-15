#!/usr/bin/env bash
#
# Download the westend relay-chain runtime wasm that `upgrade_boundary.rs` upgrades *to*.
#
# Why a downloaded blob rather than one built here: producing a runtime with a different
# `spec_version` from the one a node already runs means building polkadot-sdk, which is an
# hour of CPU for a 1.8 MB artifact that Parity already publishes as a release asset.
#
# The version must be NEWER than the runtime `networks/upgrade-boundary.toml` starts on
# (stable2603 = 1022002), because runtime storage migrations only run forward. Keep the two
# in step: if the network config's image moves, move VERSION with it.
#
# Usage:  ./scripts/fetch-westend-runtime.sh
set -euo pipefail

RELEASE="polkadot-stable2606"
VERSION="1024001"
NAME="westend_runtime-v${VERSION}.compact.compressed.wasm"
URL="https://github.com/paritytech/polkadot-sdk/releases/download/${RELEASE}/${NAME}"

DEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.zombienet/runtimes"
DEST="${DEST_DIR}/${NAME}"

if [[ -f "$DEST" ]]; then
    echo "already present: $DEST"
    exit 0
fi

mkdir -p "$DEST_DIR"
echo "downloading $NAME from $RELEASE ..."
curl -fsSL -o "$DEST" "$URL"

# A Substrate compressed runtime starts with an 8-byte magic prefix. Checking it here turns
# an HTML error page saved under a .wasm name into a failure now rather than an opaque
# `set_code` rejection later.
MAGIC="$(head -c 8 "$DEST" | xxd -p)"
if [[ "$MAGIC" != "52bc537646db8e05" ]]; then
    echo "error: $DEST is not a compressed Substrate runtime (magic: $MAGIC)" >&2
    rm -f "$DEST"
    exit 1
fi

echo "ok: $DEST ($(wc -c < "$DEST") bytes)"
