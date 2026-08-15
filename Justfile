# Polkadot Indexer Framework (PIF) — task runner.
#   just            list every recipe
#   just up         start Postgres + dev node
#   just index      index the configured chains
#
# Cargo lives on an external volume here, so PATH is set explicitly rather than relying on
# the shell profile (a non-interactive shell does not always pick it up).

export PATH := env_var_or_default("CARGO_HOME", env_var("HOME") / ".cargo") / "bin:" + env_var("PATH")

DATABASE_URL := env_var_or_default("DATABASE_URL", "postgres://indexer:indexer@localhost:5433/substrate_indexer")
PSQL := "PGPASSWORD=indexer psql -h localhost -p 5433 -U indexer -d substrate_indexer"

# All features that exist, for the "everything on" recipes.
ALL := "--features api,handler-balances,handler-identity"

default:
    @just --list --unsorted

# ---------------------------------------------------------------- environment

# Start Postgres and the dev node.
up:
    docker compose up -d
    @echo "waiting for postgres..."
    @until docker compose exec -T postgres pg_isready -U indexer -d substrate_indexer >/dev/null 2>&1; do sleep 1; done
    @echo "ready: postgres :5433, node :9944"

# Stop the stack, keeping volumes.
down:
    docker compose down

# Stop the stack and delete its data. The node restarts from genesis afterwards.
reset:
    docker compose down -v
    @echo "volumes removed; run `just up && just migrate`"

# Tail the dev node's logs.
logs:
    docker compose logs -f node

# Is the chain actually finalizing? A stalled chain makes the indexer look broken.
health:
    #!/usr/bin/env bash
    set -euo pipefail
    rpc() { curl -s -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$1\",\"params\":[]}" http://127.0.0.1:9944; }
    num() { curl -s -H 'Content-Type: application/json' \
        -d "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"chain_getHeader\",\"params\":[\"$1\"]}" \
        http://127.0.0.1:9944 | python3 -c "import sys,json;print(int(json.load(sys.stdin)['result']['number'],16))"; }
    a=$(rpc chain_getFinalizedHead | python3 -c "import sys,json;print(json.load(sys.stdin)['result'])")
    echo "chain:     $(rpc system_chain | python3 -c 'import sys,json;print(json.load(sys.stdin)["result"])')"
    echo "finalized: $(num $a)"
    sleep 8
    b=$(rpc chain_getFinalizedHead | python3 -c "import sys,json;print(json.load(sys.stdin)['result'])")
    if [ "$(num $b)" -gt "$(num $a)" ]; then echo "status:    FINALIZING ($(num $a) -> $(num $b))";
    else echo "status:    STALLED at $(num $a)"; exit 1; fi

# ---------------------------------------------------------------- running

# Apply database migrations.
migrate:
    cargo run -p polkadot-indexer-cli {{ALL}} -- migrate

# Index every chain in config/chains.toml, following the finalized head.
index *ARGS:
    cargo run -p polkadot-indexer-cli {{ALL}} -- index {{ARGS}}

# Index a bounded range, e.g. `just index-to 20`.
index-to TO:
    cargo run -p polkadot-indexer-cli {{ALL}} -- index --to {{TO}}

# Index with the light-client source available. Separate from `index` because smoldot is a
# ~100-crate dependency and the first build of it is slow.
index-light *ARGS:
    cargo run -p polkadot-indexer-cli {{ALL}},light-client -- index {{ARGS}}

# ---------------------------------------------------------------- the block archive
#
# `index` runs both stages together and is still the normal way to run the indexer. These
# split it, which is useful when the two need to move independently — archiving through a
# runtime the digest cannot read yet, or re-processing without re-downloading.

# Archive blocks locally without processing them. Decodes nothing.
fetch *ARGS:
    cargo run -p polkadot-indexer-cli {{ALL}} -- fetch {{ARGS}}

# Process blocks that are already archived. Needs no node unless a handler reads state.
digest *ARGS:
    cargo run -p polkadot-indexer-cli {{ALL}} -- digest {{ARGS}}

# Re-process a range from the archive, e.g. `just replay 0 1000`.
#
# This is what the archive is for: adding a handler or fixing a decode bug costs a
# re-digest, not a re-download.
replay FROM TO:
    cargo run -p polkadot-indexer-cli {{ALL}} -- replay --from {{FROM}} --to {{TO}}

# What the archive holds, and how far each stage has got.
store-status *ARGS:
    cargo run -p polkadot-indexer-cli {{ALL}} -- store status {{ARGS}}

# Serve the GraphQL API (GraphiQL on http://localhost:8000).
serve *ARGS:
    cargo run -p polkadot-indexer-cli {{ALL}} -- serve {{ARGS}}

# One-shot: fresh stack, migrated, indexed to block 20.
demo: up migrate (index-to "20")
    @echo "done — try `just transfers <chain-id>` or `just serve`"

# ---------------------------------------------------------------- inspecting

# Chains this indexer knows about, and how far each has been indexed.
status:
    @{{PSQL}} -c "SELECT c.id, c.name, min(b.number) AS first, max(b.number) AS last, count(b.*) AS blocks \
        FROM chains c LEFT JOIN blocks b ON b.chain_id = c.id GROUP BY c.id, c.name ORDER BY c.id;"

# Transfers recorded by the typed overlay.
transfers CHAIN:
    @{{PSQL}} -c "SELECT block_number, event_idx, from_address, to_address, amount \
        FROM transfers WHERE chain_id = '{{CHAIN}}' ORDER BY block_number DESC LIMIT 20;"

# Regenerate the genesis override that makes Alice identity registrar #0 on People.
# Needs a People node running; derives both key and value from live metadata.
gen-registrar:
    cargo test -p pif-e2e --features handler-identity \
        --test gen_registrar_override -- --ignored --nocapture

# Spawn the three-chain demo network: relay + Asset Hub + People.
#
# Slow: the node images are linux/amd64 and run under emulation, and zombienet has to build
# a raw chain spec for each parachain first. Budget ~5 minutes.
zn-up:
    @mkdir -p .zombienet
    bin/zombie-cli spawn "$PWD/crates/pif-e2e/networks/three-chain.toml" \
        --provider docker --dir "$PWD/.zombienet/out"

# Tear the demo network down.
zn-down:
    -pkill -f "zombie-cli spawn" 2>/dev/null || true
    -docker ps --format '{{{{.Names}}}}' | grep -i zombie | xargs -r docker rm -f
    -rm -rf .zombienet

# Spawn the runtime-upgrade-boundary network: ONE relay node on a deliberately old release.
#
# Fast compared to `zn-up` (one node, no parachain specs to build): ~40 seconds. Coexists
# with the three-chain network -- it binds 9977, not 9944.
zn-upgrade-up:
    @mkdir -p .zombienet
    ./scripts/fetch-westend-runtime.sh
    bin/zombie-cli spawn "$PWD/crates/pif-e2e/networks/upgrade-boundary.toml" \
        --provider docker --dir "$PWD/.zombienet/upgrade-out"

# The runtime-upgrade boundary test (IPD-002 §9.1.2).
#
# Needs a FRESH network every run: the test upgrades the chain, so a second run against the
# same one has nothing left to upgrade to. `zn-upgrade-up` first, every time.
test-upgrade-boundary:
    cargo test -p pif-e2e --test upgrade_boundary -- --ignored --nocapture

# The offline-decode spike (IPD-002 §9.1.1). Runs against any chain; 9944 by default.
test-offline-decode:
    cargo test -p pif-e2e --test decode_stored_spike -- --ignored --nocapture

# The pipeline split end to end (IPD-002 phases 1 and 2). Needs `just up`.
#
# The test that matters most is `the_split_pipeline_writes_the_same_rows`: the archive is
# worth nothing if the blocks it yields differ from the ones the network yielded. The two
# that matter next replay against a dead address — once for the dynamic core, once with a
# handler that reads chain state — so a replay that quietly became a re-download would fail
# rather than pass slowly.
test-pipeline-split: up
    cargo test -p pif-e2e --test pipeline_split -- --ignored --nocapture --test-threads=1

# The alias cross-check end to end: transfers on the hub, identities on People, one join.
zn-alias-demo:
    cargo test -p pif-e2e --features handler-balances,handler-identity \
        --test three_chain_alias -- --ignored --nocapture

# Identities indexed for a chain, most recently changed first.
identities CHAIN:
    @{{PSQL}} -c "SELECT account, effective_display AS display, username, effective_verified AS verified \
        FROM identity_current WHERE chain_id = '{{CHAIN}}' \
        ORDER BY effective_verified DESC, account LIMIT 20;"

# Everything known about one wallet's alias -- the cross-check, from the shell.
alias CHAIN ACCOUNT:
    @{{PSQL}} -c "SELECT account, effective_display AS display, username, \
        effective_verified AS verified, super_account, sub_label, judgements \
        FROM identity_current WHERE chain_id = '{{CHAIN}}' AND account = '{{ACCOUNT}}';"

# How many accounts carry a registrar-vouched identity.
verified CHAIN:
    @{{PSQL}} -c "SELECT count(*) FILTER (WHERE effective_verified) AS verified, count(*) AS total \
        FROM identity_current WHERE chain_id = '{{CHAIN}}';"

# The same transfers as the dynamic core stored them (works without handler-balances).
transfer-events CHAIN:
    @{{PSQL}} -c "SELECT block_number, idx, fields FROM events \
        WHERE chain_id = '{{CHAIN}}' AND pallet = 'Balances' AND variant = 'Transfer' \
        ORDER BY block_number DESC LIMIT 20;"

# Most common events on a chain — a quick sanity check that decoding worked.
events CHAIN:
    @{{PSQL}} -c "SELECT pallet, variant, count(*) FROM events \
        WHERE chain_id = '{{CHAIN}}' GROUP BY 1,2 ORDER BY 3 DESC LIMIT 20;"

# Verify the stored chain has no holes. Should print 0 for every chain — except one
# followed by a light client, which cannot backfill what it missed while stopped.
gaps:
    @{{PSQL}} -c "SELECT c.id, (SELECT count(*) FROM generate_series( \
            (SELECT min(number) FROM blocks WHERE chain_id = c.id), \
            (SELECT max(number) FROM blocks WHERE chain_id = c.id)) n \
          WHERE NOT EXISTS (SELECT 1 FROM blocks b WHERE b.chain_id = c.id AND b.number = n)) AS gaps \
        FROM chains c ORDER BY c.id;"

# Open a psql shell.
psql:
    @{{PSQL}}

# ---------------------------------------------------------------- testing

# Unit tests only — no Docker, no database.
test:
    cargo test --workspace --all-features

# End-to-end tests against the compose stack. Needs `just up`.
test-e2e: up
    cargo test -p pif-e2e --features handler-balances --test live_node -- --ignored --nocapture --test-threads=1

# Zombienet suites: each spawns its own throwaway network. Needs `just fetch-zombie-cli`.
test-zombienet: up
    cargo test -p pif-e2e --features handler-balances --test zombienet -- --ignored --nocapture
    cargo test -p pif-e2e --features handler-balances --test zombienet_transfers -- --ignored --nocapture

# Everything.
test-all: test test-e2e test-zombienet

# Download the prebuilt zombie-cli binary (see scripts/fetch-zombie-cli.sh for why).
fetch-zombie-cli:
    ./scripts/fetch-zombie-cli.sh

# Download chain specs for the light-client source into config/specs/.
fetch-chain-specs *SPECS:
    ./scripts/fetch-chain-specs.sh {{SPECS}}

# ---------------------------------------------------------------- quality

fmt:
    cargo fmt

# Lint every feature combination that ships.
#
# A feature-gated crate can break in one configuration while passing in another — an unused
# import behind a `cfg` is invisible until you build without that feature.
lint:
    cargo fmt --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo clippy --workspace --all-targets --features api -- -D warnings
    cargo clippy --workspace --all-targets --features handler-identity -- -D warnings
    cargo clippy --workspace --all-targets --features light-client -- -D warnings
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# What CI should run.
ci: lint test

# Remove any zombienet containers left behind by an interrupted test.
clean-zombie:
    -docker rm -f $(docker ps -aq --filter "name=zombie-") 2>/dev/null
    -pkill -f "zombie-cli spawn" 2>/dev/null
    @echo "cleaned"
