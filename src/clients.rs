use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::Instant;
use tokio::sync::watch;

use crate::protocol::RespVersion;
use crate::pubsub::PubSubHub;

struct ClientEntry {
    id: u64,
    addr: String,
    name: Option<String>,
    db: i64,
    proto: RespVersion,
    created_at: Instant,
    kill_tx: watch::Sender<bool>,
}

/// Single-threaded connection registry owned by the background worker's `LocalSet`.
///
/// Powers multi-connection `CLIENT LIST`, `CLIENT INFO`, `CLIENT KILL`, `INFO clients`,
/// and `pg_redis.max_clients` admission control.
pub struct ClientRegistry {
    clients: RefCell<BTreeMap<u64, ClientEntry>>,
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self {
            clients: RefCell::new(BTreeMap::new()),
        }
    }

    pub fn register(&self, id: u64, addr: String, kill_tx: watch::Sender<bool>) {
        self.clients.borrow_mut().insert(
            id,
            ClientEntry {
                id,
                addr,
                name: None,
                db: 0,
                proto: RespVersion::Resp2,
                created_at: Instant::now(),
                kill_tx,
            },
        );
    }

    pub fn remove(&self, id: u64) {
        self.clients.borrow_mut().remove(&id);
    }

    pub fn count(&self) -> usize {
        self.clients.borrow().len()
    }

    pub fn set_name(&self, id: u64, name: Option<String>) {
        if let Some(entry) = self.clients.borrow_mut().get_mut(&id) {
            entry.name = name;
        }
    }

    pub fn set_db(&self, id: u64, db: i64) {
        if let Some(entry) = self.clients.borrow_mut().get_mut(&id) {
            entry.db = db;
        }
    }

    pub fn set_proto(&self, id: u64, proto: RespVersion) {
        if let Some(entry) = self.clients.borrow_mut().get_mut(&id) {
            entry.proto = proto;
        }
    }

    fn format_entry(entry: &ClientEntry, pubsub: Option<&PubSubHub>) -> String {
        let name = entry.name.as_deref().unwrap_or("");
        let age = entry.created_at.elapsed().as_secs();
        let sub = pubsub
            .map(|h| h.channel_subscription_count(entry.id))
            .unwrap_or(0);
        let psub = pubsub
            .map(|h| h.pattern_subscription_count(entry.id))
            .unwrap_or(0);
        let flags = if sub + psub > 0 { "P" } else { "N" };
        let resp = match entry.proto {
            RespVersion::Resp2 => 2,
            RespVersion::Resp3 => 3,
        };
        format!(
            "id={} addr={} name={} age={} flags={} db={} sub={} psub={} resp={}\n",
            entry.id, entry.addr, name, age, flags, entry.db, sub, psub, resp
        )
    }

    pub fn format_one(&self, id: u64, pubsub: Option<&PubSubHub>) -> Option<String> {
        self.clients
            .borrow()
            .get(&id)
            .map(|e| Self::format_entry(e, pubsub))
    }

    pub fn format_list(
        &self,
        filter_ids: Option<&[u64]>,
        filter_type: Option<&str>,
        pubsub: Option<&PubSubHub>,
    ) -> String {
        let map = self.clients.borrow();
        let mut out = String::new();
        for entry in map.values() {
            if let Some(ids) = filter_ids {
                if !ids.contains(&entry.id) {
                    continue;
                }
            }
            if let Some(ty) = filter_type {
                let is_pubsub = pubsub
                    .map(|h| h.subscription_count(entry.id) > 0)
                    .unwrap_or(false);
                match ty {
                    "pubsub" if !is_pubsub => continue,
                    "normal" if is_pubsub => continue,
                    "master" | "replica" | "slave" => continue,
                    _ => {}
                }
            }
            out.push_str(&Self::format_entry(entry, pubsub));
        }
        out
    }

    pub fn kill_by_id(&self, id: u64) -> i64 {
        if let Some(entry) = self.clients.borrow().get(&id) {
            let _ = entry.kill_tx.send(true);
            1
        } else {
            0
        }
    }

    pub fn kill_by_addr(&self, addr: &str) -> i64 {
        let mut killed = 0;
        for entry in self.clients.borrow().values() {
            if entry.addr == addr {
                let _ = entry.kill_tx.send(true);
                killed += 1;
            }
        }
        killed
    }
}

/// RAII guard that removes a client from `ClientRegistry` on disconnect.
pub struct ClientRegistryGuard<'a> {
    client_id: u64,
    registry: &'a ClientRegistry,
}

impl<'a> ClientRegistryGuard<'a> {
    pub fn new(client_id: u64, registry: &'a ClientRegistry) -> Self {
        Self {
            client_id,
            registry,
        }
    }
}

impl Drop for ClientRegistryGuard<'_> {
    fn drop(&mut self) {
        self.registry.remove(self.client_id);
    }
}
