use bytes::Bytes;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

use crate::protocol::RespValue;

/// Binary-safe Pub/Sub message delivered to a subscribed connection.
#[derive(Clone, Debug)]
pub struct PubSubMessage {
    pub channel: Bytes,
    pub payload: Bytes,
}

impl PubSubMessage {
    pub fn into_resp(self) -> RespValue {
        RespValue::Push(vec![
            RespValue::bulk("message"),
            RespValue::BulkString(self.channel),
            RespValue::BulkString(self.payload),
        ])
    }
}

struct ClientPubSub {
    sender: mpsc::UnboundedSender<PubSubMessage>,
    channels: Vec<Bytes>,
    channel_set: HashSet<Bytes>,
}

struct PubSubState {
    channels: HashMap<Bytes, HashSet<u64>>,
    clients: HashMap<u64, ClientPubSub>,
}

/// In-process zero-copy Pub/Sub broker running on the background worker's `LocalSet`.
///
/// Supports arbitrary binary payloads without PostgreSQL's 8KB `NOTIFY` limit,
/// strict per-publisher FIFO delivery, exact subscriber counts, and RAII cleanup on disconnect.
pub struct PubSubHub {
    state: RefCell<PubSubState>,
}

impl PubSubHub {
    pub fn new() -> Self {
        Self {
            state: RefCell::new(PubSubState {
                channels: HashMap::new(),
                clients: HashMap::new(),
            }),
        }
    }

    pub fn register_client(&self, client_id: u64, sender: mpsc::UnboundedSender<PubSubMessage>) {
        let mut st = self.state.borrow_mut();
        st.clients.insert(
            client_id,
            ClientPubSub {
                sender,
                channels: Vec::new(),
                channel_set: HashSet::new(),
            },
        );
    }

    /// Subscribe `client_id` to `channel`. Duplicate subscriptions are idempotent and
    /// return the unchanged subscription count.
    pub fn subscribe(&self, client_id: u64, channel: Bytes) -> usize {
        let mut st = self.state.borrow_mut();
        let Some(client) = st.clients.get_mut(&client_id) else {
            return 0;
        };
        let is_new = client.channel_set.insert(channel.clone());
        if is_new {
            client.channels.push(channel.clone());
        }
        let total = client.channels.len();
        if is_new {
            st.channels.entry(channel).or_default().insert(client_id);
        }
        total
    }

    /// Unsubscribe `client_id` from a single `channel`. Returns remaining subscription count.
    pub fn unsubscribe(&self, client_id: u64, channel: &Bytes) -> usize {
        let mut st = self.state.borrow_mut();
        let (removed, remaining) = match st.clients.get_mut(&client_id) {
            Some(client) => {
                let removed = client.channel_set.remove(channel);
                if removed {
                    client.channels.retain(|c| c != channel);
                }
                (removed, client.channels.len())
            }
            None => (false, 0),
        };

        if removed {
            let mut empty = false;
            if let Some(subs) = st.channels.get_mut(channel) {
                subs.remove(&client_id);
                empty = subs.is_empty();
            }
            if empty {
                st.channels.remove(channel);
            }
        }

        remaining
    }

    /// Unsubscribe `client_id` from all channels, returning `(channel, remaining_count)`
    /// in subscription order down to `0`.
    pub fn unsubscribe_all(&self, client_id: u64) -> Vec<(Bytes, usize)> {
        let mut st = self.state.borrow_mut();
        let removed_channels = match st.clients.get_mut(&client_id) {
            Some(client) => {
                client.channel_set.clear();
                std::mem::take(&mut client.channels)
            }
            None => Vec::new(),
        };

        let mut acks = Vec::with_capacity(removed_channels.len());
        let mut remaining = removed_channels.len();

        for ch in removed_channels {
            remaining -= 1;
            let mut empty = false;
            if let Some(subs) = st.channels.get_mut(&ch) {
                subs.remove(&client_id);
                empty = subs.is_empty();
            }
            if empty {
                st.channels.remove(&ch);
            }
            acks.push((ch, remaining));
        }

        acks
    }

    pub fn subscription_count(&self, client_id: u64) -> usize {
        self.state
            .borrow()
            .clients
            .get(&client_id)
            .map(|c| c.channels.len())
            .unwrap_or(0)
    }

    /// Publish `payload` to all live subscribers on `channel`.
    /// Returns the exact number of clients that received the message.
    pub fn publish(&self, channel: &Bytes, payload: &Bytes) -> i64 {
        let mut dead_clients = Vec::new();
        let mut delivered: i64 = 0;

        {
            let st = self.state.borrow();
            if let Some(subs) = st.channels.get(channel) {
                for &cid in subs {
                    if let Some(client) = st.clients.get(&cid) {
                        let msg = PubSubMessage {
                            channel: channel.clone(),
                            payload: payload.clone(),
                        };
                        if client.sender.send(msg).is_ok() {
                            delivered += 1;
                        } else {
                            dead_clients.push(cid);
                        }
                    } else {
                        dead_clients.push(cid);
                    }
                }
            }
        }

        for cid in dead_clients {
            self.remove_client(cid);
        }

        delivered
    }

    /// Remove a disconnected client and prune all of its channel subscriptions.
    pub fn remove_client(&self, client_id: u64) {
        let mut st = self.state.borrow_mut();
        if let Some(client) = st.clients.remove(&client_id) {
            for ch in client.channels {
                let mut empty = false;
                if let Some(subs) = st.channels.get_mut(&ch) {
                    subs.remove(&client_id);
                    empty = subs.is_empty();
                }
                if empty {
                    st.channels.remove(&ch);
                }
            }
        }
    }

    /// List active channels (used by `PUBSUB CHANNELS [pattern]`).
    pub fn active_channels(&self, pattern: Option<&str>) -> Vec<Bytes> {
        let st = self.state.borrow();
        let mut out: Vec<Bytes> = st
            .channels
            .keys()
            .filter(|ch| match pattern {
                None => true,
                Some(pat) => {
                    let s = String::from_utf8_lossy(ch);
                    crate::commands::keys::glob_match(pat, &s)
                }
            })
            .cloned()
            .collect();
        out.sort();
        out
    }

    /// Return subscriber counts for requested channels (`PUBSUB NUMSUB [ch ...]`).
    pub fn numsub(&self, channels: &[Bytes]) -> Vec<(Bytes, i64)> {
        let st = self.state.borrow();
        channels
            .iter()
            .map(|ch| {
                let count = st.channels.get(ch).map(|s| s.len() as i64).unwrap_or(0);
                (ch.clone(), count)
            })
            .collect()
    }
}

/// RAII guard that guarantees immediate subscription cleanup when a TCP connection terminates.
pub struct PubSubClientGuard<'a> {
    client_id: u64,
    hub: &'a PubSubHub,
}

impl<'a> PubSubClientGuard<'a> {
    pub fn new(client_id: u64, hub: &'a PubSubHub) -> Self {
        Self { client_id, hub }
    }
}

impl Drop for PubSubClientGuard<'_> {
    fn drop(&mut self) {
        self.hub.remove_client(self.client_id);
    }
}
