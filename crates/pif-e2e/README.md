# indexer-e2e

End-to-end tests. Everything here is `#[ignore]`d, so `cargo test --workspace` stays
hermetic — no Docker, no database, no network.

## Suites

| File | Needs | Proves |
|---|---|---|
| `tests/live_node.rs` | compose stack | resume-after-restart, dynamic decoding, typed-overlay projection of a real transfer |
| `tests/zombienet.rs` | `bin/zombie-cli` + Docker | the same binary indexes a chain created minutes ago, with a genesis that did not exist at build time |
| `tests/zombienet_transfers.rs` | `bin/zombie-cli` + Docker | transfers submitted on that fresh chain are recorded by **both** the dynamic core and the typed overlay, with exact `u128` amounts |
| `tests/three_chain_alias.rs` | `just zn-up` + Postgres | relay + Asset Hub + People: hub transfers resolve to People-chain display names through one cross-chain join |
| `tests/people_metadata.rs` | a People node | the `identity` handler's storage-item names and decoding actually match a real People runtime |
| `tests/gen_registrar_override.rs` | a People node | *generator*: rebuilds the genesis override that installs Alice as registrar #0 (`just gen-registrar`) |

Each zombienet suite lives in its own test binary because cargo runs test binaries
sequentially but threads tests *within* one — two networks must never spawn concurrently.

## Running

```sh
docker compose up -d                      # Postgres on :5433, dev node on :9944
./scripts/fetch-zombie-cli.sh             # once

cargo test -p indexer-e2e --features handler-balances -- --ignored --nocapture
```

Tests skip themselves (rather than fail) when `zombie-cli` or the Docker daemon is missing.

### Funding dev accounts

`zombienet_transfers.rs` sets `initial_balance` on its validator explicitly:

```toml
[[relaychain.nodes]]
name = "alice"
validator = true
initial_balance = 1000000000000000000
```

zombienet's default node balance is roughly `2e12`, which is not enough for the test's
largest transfer (2^53 + 1, chosen to prove precision is not lost). Without this, the
second transfer fails with `Token error: Funds are unavailable`.

## Why zombienet is driven through the CLI, not the SDK

`zombienet-sdk` is **not** a dependency of this crate, for two independent reasons:

1. **It cannot be built.** Every `zombienet-sdk` 0.4.x resolves to
   `multihash 0.17 → core2 ^0.4`, and *every published version of `core2` is yanked*.
   Cargo cannot select a version, so the whole 0.4 line fails to resolve. (0.3.x resolves
   but predates the `spawn_docker` API.)
2. **It would fork subxt.** The SDK depends on `subxt ^0.44` while this workspace is on
   `subxt 0.50`. Both can coexist in a lockfile, but their types are unrelated — an
   `OnlineClient` from one cannot be used by the other.

Instead, `src/zombienet.rs` runs the prebuilt `zombie-cli` binary as a subprocess and reads
node endpoints from the `zombie.json` file it writes. The only thing crossing that boundary
is JSON, so there is no second subxt tree at all. The macOS build of `zombie-cli` is native
arm64, so the orchestrator itself does not run under emulation — only the node containers do.

## Known limitation: multi-node networks do not peer on this setup

`tests/zombienet.rs` spawns a **single-validator** relay chain. That is not arbitrary — a
multi-node network never reaches `peers > 0` here, so it sits at block 0 forever and
finalizes nothing. Two separate causes were found, and only the first is fixable from config.

Environment: `zombie-cli` v0.4.15, macOS/arm64, a non-Docker-Desktop daemon (aarch64 Linux
VM), `parity/polkadot:stable2606` (`linux/amd64`, under emulation).

### Cause 1 — wrong bootnode port (fixable)

By default the Docker provider builds each bootnode multiaddr from the node's **host-mapped**
port while the node listens on its **container** port:

```
alice listens:  --listen-addr /ip4/0.0.0.0/tcp/30333/ws
bob is told:    --bootnodes /ip4/172.17.0.2/tcp/53892/ws/p2p/12D3KooW...
                                         ^^^^^ host port, not 30333
```

Pinning `p2p_port` per node in the network TOML fixes this — the generated address then
correctly reads `/ip4/172.17.0.2/tcp/30333/ws`:

```toml
[[relaychain.nodes]]
name = "alice"
validator = true
p2p_port = 30333

[[relaychain.nodes]]
name = "bob"
validator = true
p2p_port = 30334
```

### Cause 2 — libp2p Noise handshake fails (not fixed)

With the address corrected, both containers on the same bridge network and mutually
routable, peering still fails at the encryption layer:

```
failed to decrypt message ty=WebSocket peer=PeerId("12D3KooW...")
    buf_len=1929 frame_size=1945 error=Decrypt
💤 Idle (0 peers), best: #0 (0xecc9…6601), finalized #0
```

The 16-byte gap between `frame_size` and `buf_len` is the Poly1305 authentication tag, so
this is an AEAD authentication failure during the Noise handshake — the frames arrive but do
not authenticate. The most likely explanation is the `linux/amd64` node binary running under
emulation on arm64 (mistranslated SIMD/AES paths), but that has **not** been confirmed; it
was not pursued further because the single-validator topology fully exercises the indexer.

### Consequence

A single validator needs no peer connection: it authors and finalizes alone and exercises
the indexer identically, which is why `tests/zombienet.rs` uses one.

**Connected parachain topologies remain blocked**, since a collator must reach the relay
chain over p2p to receive relay-parent data and return collations.

### But parachains still run here — they just author alone

`networks/three-chain.toml` spawns a relay **and** two parachains (Asset Hub + People) that
all produce and finalize blocks on this setup, despite the Noise errors still appearing in
every node's log. The trick is `--dev-block-time`, which makes a parachain node author its
own blocks on a timer instead of waiting on the relay:

```toml
[parachains.collator]
args = ["--dev-block-time=1000"]
```

Nothing in that network needs a peer, so nothing in it is blocked by the handshake failure.
`tests/three_chain_alias.rs` runs against it, exercising every path the identity handler has:
transfers on the hub; identities, registrar judgements, usernames (granted, queued, accepted,
made primary) and sub-identities on People; and one SQL join resolving hub addresses to
People-chain names.

### Adding a registrar without sudo

`Identity::add_registrar` needs a root origin. The People chain has no `Sudo` pallet — that
origin normally arrives from the relay as an XCM `Transact`, which this network cannot deliver
since its parachains are never backed. `pallet_identity` has no `GenesisConfig` for registrars
either.

The same is true of `add_username_authority`. So the network spec installs both directly into
genesis storage — Alice as registrar #0 and as the authority for the `.pif` suffix:

```toml
[[parachains]]
id = 1004
chain = "people-westend-local"
raw_spec_override = "crates/pif-e2e/networks/people-registrar.json"
```

`raw_spec_override` deep-merges JSON into the *raw* chain spec, which is a map of hex storage
key to hex value, so the file just sets `Identity::Registrars`. Regenerate it with
`just gen-registrar` — the key comes from subxt's own `key_prefix()` and the value is encoded
against the runtime's `RegistrarInfo` type, so neither is hand-rolled hex.

**What this is not.** The parachains are registered on the relay but their blocks are never
backed or included there — no collation can be sent without p2p. So there is no shared
security, no relay-driven finality, and **no XCM**. It is three independent chains that
zombienet starts together, which is exactly enough for a `chain_id`-keyed database join and
not enough for anything testing real parachain consensus or cross-chain messaging.

For those, run on **Linux x86_64** with `--provider native` and released
`polkadot` / `polkadot-parachain` binaries — no emulation, and the native provider sidesteps
container port mapping entirely. Parity publishes no macOS node binaries, so doing that
locally would mean building polkadot-sdk from source (~40–90 min).
