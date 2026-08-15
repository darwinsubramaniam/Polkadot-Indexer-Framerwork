//! Driving zombienet through the prebuilt `zombie-cli` binary.
//!
//! We deliberately do not use the `zombienet-sdk` crate — see this crate's `Cargo.toml`.
//! Instead `zombie-cli` is run as a subprocess and the node endpoints are read from the
//! `zombie.json` state file it writes into the network's base directory. The only thing
//! crossing the boundary is JSON.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::zombie_cli;

/// How long to wait for `zombie.json` to appear. Node images are `linux/amd64` and run
/// under emulation on Apple Silicon, so this is deliberately generous.
const SPAWN_TIMEOUT: Duration = Duration::from_secs(300);

/// A spawned zombienet network. Dropping it tears the network down.
pub struct Network {
    child: Child,
    base_dir: PathBuf,
    namespace: String,
    nodes: Vec<Node>,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub name: String,
    pub ws_uri: String,
}

impl Network {
    /// Spawn a network from a TOML spec using the Docker provider.
    ///
    /// Returns `Ok(None)` when the environment cannot run zombienet (no `zombie-cli`, no
    /// Docker), so callers can skip rather than fail — these tests are opt-in.
    pub fn spawn(spec_toml: &str, base_dir: &Path) -> Result<Option<Self>> {
        let Some(cli) = zombie_cli() else {
            eprintln!("skipping: bin/zombie-cli not found; run scripts/fetch-zombie-cli.sh");
            return Ok(None);
        };
        if !docker_available() {
            eprintln!("skipping: docker daemon is not reachable");
            return Ok(None);
        }

        std::fs::create_dir_all(base_dir)?;
        let spec_path = base_dir.join("network.toml");
        std::fs::write(&spec_path, spec_toml)?;

        let out_dir = base_dir.join("out");
        // zombie-cli refuses to reuse a populated directory.
        let _ = std::fs::remove_dir_all(&out_dir);

        let child = Command::new(&cli)
            .arg("spawn")
            .arg(&spec_path)
            .args(["--provider", "docker"])
            .arg("--dir")
            .arg(&out_dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("launching {}", cli.display()))?;

        let state_file = out_dir.join("zombie.json");
        let started = Instant::now();
        while !state_file.exists() {
            if started.elapsed() > SPAWN_TIMEOUT {
                let mut child = child;
                let _ = child.kill();
                bail!(
                    "zombie.json did not appear within {:?}; check the network spec",
                    SPAWN_TIMEOUT
                );
            }
            std::thread::sleep(Duration::from_secs(2));
        }

        // The file appears before it is fully written; give the writer a moment.
        std::thread::sleep(Duration::from_secs(2));
        let (nodes, namespace) = parse_state(&state_file)?;

        Ok(Some(Self {
            child,
            base_dir: out_dir,
            namespace,
            nodes,
        }))
    }

    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Look up a node's WebSocket endpoint by name.
    pub fn ws_uri(&self, name: &str) -> Result<String> {
        self.nodes
            .iter()
            .find(|n| n.name == name)
            .map(|n| n.ws_uri.clone())
            .with_context(|| {
                let known: Vec<_> = self.nodes.iter().map(|n| &n.name).collect();
                format!("no node named {name:?}; network has {known:?}")
            })
    }

    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

impl Drop for Network {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();

        // Killing the CLI does not always reap the containers it created.
        let _ = Command::new("sh")
            .arg("-c")
            .arg(format!(
                "docker rm -f $(docker ps -aq --filter name={}) >/dev/null 2>&1",
                self.namespace
            ))
            .status();
    }
}

/// Extract node names and endpoints from `zombie.json`.
///
/// Shape (verified against zombie-cli v0.4.15):
/// ```json
/// { "ns": "zombie-<uuid>",
///   "relay":      { "nodes": [ { "name": "alice", "ws_uri": "ws://127.0.0.1:PORT" } ] },
///   "parachains": { "<para_id>": { "collators": [ ... ] } } }
/// ```
fn parse_state(path: &Path) -> Result<(Vec<Node>, String)> {
    let raw = std::fs::read_to_string(path)?;
    let json: serde_json::Value = serde_json::from_str(&raw)?;

    let namespace = json
        .get("ns")
        .and_then(|v| v.as_str())
        .unwrap_or("zombie-")
        .to_owned();

    let mut nodes = Vec::new();
    collect_nodes(json.get("relay"), &mut nodes);
    if let Some(parachains) = json.get("parachains") {
        // `parachains` is keyed by para id; each value holds its own node list.
        match parachains {
            serde_json::Value::Object(map) => {
                for para in map.values() {
                    collect_nodes(Some(para), &mut nodes);
                }
            }
            serde_json::Value::Array(items) => {
                for para in items {
                    collect_nodes(Some(para), &mut nodes);
                }
            }
            _ => {}
        }
    }

    if nodes.is_empty() {
        bail!("no nodes with a ws_uri found in {}", path.display());
    }
    Ok((nodes, namespace))
}

/// Pull `{name, ws_uri}` pairs out of any `nodes`/`collators` array under `container`.
fn collect_nodes(container: Option<&serde_json::Value>, out: &mut Vec<Node>) {
    let Some(container) = container else { return };

    for key in ["nodes", "collators"] {
        let Some(list) = container.get(key).and_then(|v| v.as_array()) else {
            continue;
        };
        for node in list {
            let (Some(name), Some(ws_uri)) = (
                node.get("name").and_then(|v| v.as_str()),
                node.get("ws_uri").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            out.push(Node {
                name: name.to_owned(),
                ws_uri: ws_uri.to_owned(),
            });
        }
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_relay_nodes_from_zombie_json() {
        let json = serde_json::json!({
            "ns": "zombie-abc",
            "relay": { "nodes": [
                { "name": "alice", "ws_uri": "ws://127.0.0.1:1111" },
                { "name": "bob",   "ws_uri": "ws://127.0.0.1:2222" }
            ]},
            "parachains": {}
        });

        let dir = std::env::temp_dir().join("zn-parse-relay");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("zombie.json");
        std::fs::write(&path, json.to_string()).unwrap();

        let (nodes, ns) = parse_state(&path).unwrap();
        assert_eq!(ns, "zombie-abc");
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].name, "alice");
        assert_eq!(nodes[1].ws_uri, "ws://127.0.0.1:2222");
    }

    #[test]
    fn parses_parachain_collators_too() {
        let json = serde_json::json!({
            "ns": "zombie-xyz",
            "relay": { "nodes": [{ "name": "alice", "ws_uri": "ws://127.0.0.1:1111" }] },
            "parachains": {
                "1000": { "collators": [{ "name": "collator01", "ws_uri": "ws://127.0.0.1:3333" }] }
            }
        });

        let dir = std::env::temp_dir().join("zn-parse-para");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("zombie.json");
        std::fs::write(&path, json.to_string()).unwrap();

        let (nodes, _) = parse_state(&path).unwrap();
        let names: Vec<_> = nodes.iter().map(|n| n.name.as_str()).collect();
        assert!(names.contains(&"alice"), "got {names:?}");
        assert!(names.contains(&"collator01"), "got {names:?}");
    }

    #[test]
    fn nodes_without_endpoints_are_skipped_not_fatal() {
        let json = serde_json::json!({
            "ns": "zombie-1",
            "relay": { "nodes": [
                { "name": "no-endpoint-yet" },
                { "name": "alice", "ws_uri": "ws://127.0.0.1:1111" }
            ]}
        });

        let dir = std::env::temp_dir().join("zn-parse-partial");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("zombie.json");
        std::fs::write(&path, json.to_string()).unwrap();

        let (nodes, _) = parse_state(&path).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].name, "alice");
    }
}
