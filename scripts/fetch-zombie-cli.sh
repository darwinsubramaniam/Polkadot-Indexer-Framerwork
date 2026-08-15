#!/usr/bin/env bash
# Fetch the prebuilt zombie-cli binary used by the e2e suite.
#
# We drive zombienet through this binary rather than the `zombienet-sdk` crate:
# the 0.4.x crate line cannot be resolved from crates.io (multihash 0.17 requires
# core2 ^0.4, and every published core2 version is yanked), and it would also drag
# in a second, incompatible subxt (0.44 vs our 0.50).
#
# The darwin build is native arm64, so the orchestrator itself does not run under
# emulation — only the node containers do.
set -euo pipefail

VERSION="${ZOMBIE_CLI_VERSION:-v0.4.15}"
DEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/bin"
DEST="$DEST_DIR/zombie-cli"

case "$(uname -s)/$(uname -m)" in
    Darwin/arm64)  ASSET="zombie-cli-aarch64-apple-darwin" ;;
    Linux/x86_64)  ASSET="zombie-cli-x86_64-unknown-linux-gnu" ;;
    *)
        echo "error: no prebuilt zombie-cli for $(uname -s)/$(uname -m)" >&2
        echo "       see https://github.com/paritytech/zombienet-sdk/releases" >&2
        exit 1
        ;;
esac

BASE="https://github.com/paritytech/zombienet-sdk/releases/download/$VERSION"
mkdir -p "$DEST_DIR"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "==> downloading $ASSET ($VERSION)"
curl -sSL --fail -o "$TMP/zombie-cli" "$BASE/$ASSET"
curl -sSL --fail -o "$TMP/checksums.txt" "$BASE/checksums.txt"

echo "==> verifying checksum"
expected="$(grep "$ASSET" "$TMP/checksums.txt" | awk '{print $1}')"
if command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$TMP/zombie-cli" | awk '{print $1}')"
else
    actual="$(sha256sum "$TMP/zombie-cli" | awk '{print $1}')"
fi

if [ -z "$expected" ] || [ "$expected" != "$actual" ]; then
    echo "error: checksum mismatch for $ASSET" >&2
    echo "       expected: ${expected:-<not found in checksums.txt>}" >&2
    echo "       actual:   $actual" >&2
    exit 1
fi

install -m 0755 "$TMP/zombie-cli" "$DEST"
echo "==> installed $DEST"
"$DEST" --help >/dev/null && echo "==> ok"
