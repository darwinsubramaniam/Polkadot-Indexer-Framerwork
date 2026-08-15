#!/usr/bin/env bash
# Fetch chain specs for the light-client source into config/specs/.
#
# A light client bootstraps from a chain spec: the genesis state (or a finality checkpoint),
# the protocol id, and bootnodes to dial. Unlike an RPC url, it cannot be discovered — it is
# the thing that *identifies* the chain, so it has to be supplied.
#
# These come from substrate-connect's known-chains package, which is what the browser
# extension and every substrate-connect dapp use. The relay specs carry a `lightSyncState`
# checkpoint, so smoldot warp-syncs to the head in seconds instead of verifying every
# authority-set change since genesis.
#
# They are deliberately not committed: polkadot.json alone is ~300 KB of generated JSON, and
# a stale checkpoint only slows the first sync down.
#
#   ./scripts/fetch-chain-specs.sh                       # the defaults below
#   ./scripts/fetch-chain-specs.sh ksmcc3 kusama_people  # any name from the specs directory
#
# Full list: https://github.com/paritytech/substrate-connect/tree/main/packages/connect-known-chains/specs
set -euo pipefail

BASE="https://raw.githubusercontent.com/paritytech/substrate-connect/main/packages/connect-known-chains/specs"
DEST="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/config/specs"

# Relay first, then the parachain that the identity handler indexes.
SPECS=("${@:-}")
if [ -z "${SPECS[0]:-}" ]; then
    SPECS=(polkadot polkadot_people)
fi

mkdir -p "$DEST"

for name in "${SPECS[@]}"; do
    # `polkadot_people` upstream, `polkadot-people.json` here: the config file reads better
    # with hyphens, and nothing downstream cares what the file is called.
    out="$DEST/${name//_/-}.json"

    echo "fetching $name -> ${out#"$PWD/"}"
    if ! curl -sSfL "$BASE/$name.json" -o "$out.tmp"; then
        echo "error: no chain spec named '$name' upstream" >&2
        echo "       see $BASE" >&2
        rm -f "$out.tmp"
        exit 1
    fi

    # A spec that is not valid JSON would fail much later, inside smoldot, as an
    # "invalid chain spec" with no hint about which file caused it.
    if ! python3 -c "import json,sys; json.load(open(sys.argv[1]))" "$out.tmp" 2>/dev/null; then
        echo "error: $name.json is not valid JSON" >&2
        rm -f "$out.tmp"
        exit 1
    fi

    mv "$out.tmp" "$out"
done

echo
echo "specs in ${DEST#"$PWD/"}:"
ls -1sh "$DEST"
