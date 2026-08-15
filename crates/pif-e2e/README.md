# indexer-e2e

End-to-end tests. Everything here is `#[ignore]`d, so `cargo test --workspace` stays
hermetic — no Docker, no database, no network.

## Suites

| File | Needs | Proves |
|---|---|---|
| `tests/live_node.rs` | compose stack | resume-after-restart, dynamic decoding, typed-overlay projection of a real transfer |
| `tests/zombienet.rs` | `bin/zombie-cli` + Docker | the same binary indexes a chain created minutes ago, with a genesis that did not exist at build time |
| `tests/zombienet_transfers.rs` | `bin/zombie-cli` + Docker | transfers submitted on that fresh chain are recorded by **both** the dynamic core and the typed overlay, with exact `u128` amounts |

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
the indexer identically, which is why the test uses one. **Parachain topologies remain
blocked**, since a collator must reach the relay chain over p2p.

If parachain coverage is needed, run the suite on **Linux x86_64** with `--provider native`
and released `polkadot` / `polkadot-parachain` binaries — no emulation, and the native
provider sidesteps container port mapping entirely. Parity publishes no macOS node binaries,
so doing this locally would mean building polkadot-sdk from source (~40–90 min).
