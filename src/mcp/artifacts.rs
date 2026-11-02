use super::{ToolDefinition, config::Config};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

struct Entry {
    bytes: Bytes,
    expires: Instant,
}
#[derive(Default)]
struct Store {
    entries: HashMap<String, Entry>,
    bytes: usize,
}
pub struct Artifacts {
    config: Arc<Config>,
    store: Mutex<Store>,
}

impl Artifacts {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            store: Mutex::new(Store::default()),
        }
    }
    pub fn store(&self, bytes: Vec<u8>) -> Result<Value, String> {
        let mut store = self.store.lock().expect("result store mutex");
        Self::expire(&mut store);
        if bytes.len()
            > self
                .config
                .limits
                .max_results_bytes
                .saturating_sub(store.bytes)
        {
            return Err("result retention budget exhausted; operation may have completed. Release result handles and inspect state; do not replay writes".into());
        }
        let id = format!("r_{:032x}", rand::random::<u128>());
        let len = bytes.len();
        store.bytes += len;
        store.entries.insert(
            id.clone(),
            Entry {
                bytes: bytes.into(),
                expires: Instant::now() + Duration::from_millis(self.config.limits.result_ttl_ms),
            },
        );
        Ok(
            json!({"result_id":id,"total_bytes":len,"encoding":"json","read_tool":"ffkv_result","expires_in_ms":self.config.limits.result_ttl_ms}),
        )
    }
    pub fn call(&self, args: Value) -> Result<Value, String> {
        let args: Arguments =
            serde_json::from_value(args).map_err(|e| format!("invalid result arguments: {e}"))?;
        if !matches!(args.action.as_str(), "read" | "release") {
            return Err("result action must be read or release".into());
        }
        if !(1..=262_144).contains(&args.length) {
            return Err("result length must be between 1 and 262144".into());
        }
        let mut store = self.store.lock().expect("result store mutex");
        Self::expire(&mut store);
        if args.action == "release" {
            let entry = store
                .entries
                .remove(&args.result_id)
                .ok_or("unknown or expired result handle")?;
            store.bytes -= entry.bytes.len();
            return Ok(json!({"result_id":args.result_id,"released":true}));
        }
        let entry = store
            .entries
            .get(&args.result_id)
            .ok_or("unknown or expired result handle")?;
        if args.offset > entry.bytes.len() {
            return Err("result offset exceeds total_bytes".into());
        }
        let end = args
            .offset
            .saturating_add(args.length)
            .min(entry.bytes.len());
        // Bytes slices share the backing allocation; do not hold the store lock during base64 work.
        let bytes = entry.bytes.slice(args.offset..end);
        let total = entry.bytes.len();
        drop(store);
        Ok(
            json!({"result_id":args.result_id,"offset":args.offset,"total_bytes":total,
            "data_base64":STANDARD.encode(bytes),"next_offset":(end < total).then_some(end)}),
        )
    }
    pub fn prune(&self) {
        Self::expire(&mut self.store.lock().expect("result store mutex"));
    }
    fn expire(store: &mut Store) {
        let now = Instant::now();
        store.entries.retain(|_, entry| {
            if entry.expires <= now {
                store.bytes -= entry.bytes.len();
                false
            } else {
                true
            }
        });
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    action: String,
    result_id: String,
    #[serde(default)]
    offset: usize,
    #[serde(default = "chunk_size")]
    length: usize,
}
fn chunk_size() -> usize {
    65_536
}

pub fn definitions() -> Vec<ToolDefinition> {
    vec![ToolDefinition {
        name: "ffkv_result",
        read_only: false,
        description: "Read a bounded base64 slice of a large tool result's serialized JSON, or release its handle. Concatenate decoded slices then parse JSON. Handles expire; no silent truncation or automatic command replay.",
        schema: json!({"type":"object","additionalProperties":false,"required":["action","result_id"],"properties":{
            "action":{"type":"string","enum":["read","release"]},"result_id":{"type":"string"},
            "offset":{"type":"integer","minimum":0},"length":{"type":"integer","minimum":1,"maximum":262144}
        }}),
    }]
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paged_bytes_are_lossless_and_release_restores_capacity() {
        let mut config = Config::default();
        config.limits.max_results_bytes = 8;
        let artifacts = Artifacts::new(Arc::new(config));
        let bytes = vec![0, 255, 195, 169, 1, 2, 3, 4];
        let result = artifacts.store(bytes.clone()).unwrap();
        let id = result["result_id"].as_str().unwrap();
        assert!(artifacts.store(vec![9]).is_err());
        let mut restored = Vec::new();
        for offset in [0, 3, 6] {
            let page = artifacts
                .call(json!({"action":"read","result_id":id,"offset":offset,"length":3}))
                .unwrap();
            restored.extend(
                STANDARD
                    .decode(page["data_base64"].as_str().unwrap())
                    .unwrap(),
            );
        }
        assert_eq!(restored, bytes);
        assert!(
            artifacts
                .call(json!({"action":"read","result_id":id,"offset":9}))
                .is_err()
        );
        artifacts
            .call(json!({"action":"release","result_id":id}))
            .unwrap();
        assert!(
            artifacts
                .call(json!({"action":"read","result_id":id}))
                .is_err()
        );
        let next = artifacts.store(vec![9; 8]).unwrap();
        assert_ne!(next["result_id"], result["result_id"]);
    }
    #[test]
    fn expired_results_are_not_readable_or_charged() {
        let mut config = Config::default();
        config.limits.max_results_bytes = 4;
        let artifacts = Artifacts::new(Arc::new(config));
        let result = artifacts.store(vec![1; 4]).unwrap();
        let id = result["result_id"].as_str().unwrap();
        artifacts
            .store
            .lock()
            .unwrap()
            .entries
            .get_mut(id)
            .unwrap()
            .expires = Instant::now();
        assert!(
            artifacts
                .call(json!({"action":"read","result_id":id}))
                .is_err()
        );
        assert_eq!(artifacts.store(vec![2; 4]).unwrap()["total_bytes"], 4);
    }
}
