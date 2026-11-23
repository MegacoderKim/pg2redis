use bytes::Bytes;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

use crate::protocol::RespValue;

/// Binary-safe Pub/Sub message delivered to a subscribed connection (channel or pattern match).
#[derive(Clone, Debug)]
pub enum PubSubMessage {
    Channel {
        channel: Bytes,
        payload: Bytes,
    },
    Pattern {
        pattern: Bytes,
        channel: Bytes,
        payload: Bytes,
    },
}

impl PubSubMessage {
    pub fn into_resp(self) -> RespValue {
        match self {
            PubSubMessage::Channel { channel, payload } => RespValue::Push(vec![
                RespValue::bulk("message"),
                RespValue::BulkString(channel),
                RespValue::BulkString(payload),
            ]),
            PubSubMessage::Pattern {
                pattern,
                channel,
                payload,
            } => RespValue::Push(vec![
                RespValue::bulk("pmessage"),
                RespValue::BulkString(pattern),
                RespValue::BulkString(channel),
                RespValue::BulkString(payload),
            ]),
        }
    }
}

struct ClientPubSub {
    sender: mpsc::Sender<PubSubMessage>,
    channels: Vec<Bytes>,
    channel_set: HashSet<Bytes>,
    patterns: Vec<Bytes>,
    pattern_set: HashSet<Bytes>,
}

impl ClientPubSub {
    fn total_subscriptions(&self) -> usize {
        self.channels.len() + self.patterns.len()
    }
}

struct PubSubState {
    channels: HashMap<Bytes, HashSet<u64>>,
    patterns: HashMap<Bytes, HashSet<u64>>,
    clients: HashMap<u64, ClientPubSub>,
}

/// In-process zero-copy Pub/Sub broker running on the background worker's `LocalSet`.
///
/// Supports both named-channel (`SUBSCRIBE`/`UNSUBSCRIBE`) and glob pattern
/// (`PSUBSCRIBE`/`PUNSUBSCRIBE`) subscriptions over bounded per-client Tokio channels
/// so stalled consumers are evicted instead of growing worker memory unboundedly.
pub struct PubSubHub {
    state: RefCell<PubSubState>,
}

impl PubSubHub {
    pub fn new() -> Self {
        Self {
            state: RefCell::new(PubSubState {
                channels: HashMap::new(),
                patterns: HashMap::new(),
                clients: HashMap::new(),
            }),
        }
    }

    pub fn register_client(&self, client_id: u64, sender: mpsc::Sender<PubSubMessage>) {
        let mut st = self.state.borrow_mut();
        st.clients.insert(
            client_id,
            ClientPubSub {
                sender,
                channels: Vec::new(),
                channel_set: HashSet::new(),
                patterns: Vec::new(),
                pattern_set: HashSet::new(),
            },
        );
    }

    /// Subscribe `client_id` to named `channel`. Duplicate subscriptions are idempotent and
    /// return the unchanged total subscription count (`channels + patterns`).
    pub fn subscribe(&self, client_id: u64, channel: Bytes) -> usize {
        let mut st = self.state.borrow_mut();
        let Some(client) = st.clients.get_mut(&client_id) else {
            return 0;
        };
        let is_new = client.channel_set.insert(channel.clone());
        if is_new {
            client.channels.push(channel.clone());
        }
        let total = client.total_subscriptions();
        if is_new {
            st.channels.entry(channel).or_default().insert(client_id);
        }
        total
    }

    /// Unsubscribe `client_id` from a single named `channel`. Returns remaining total count.
    pub fn unsubscribe(&self, client_id: u64, channel: &Bytes) -> usize {
        let mut st = self.state.borrow_mut();
        let (removed, remaining) = match st.clients.get_mut(&client_id) {
            Some(client) => {
                let removed = client.channel_set.remove(channel);
                if removed {
                    client.channels.retain(|c| c != channel);
                }
                (removed, client.total_subscriptions())
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

    /// Unsubscribe `client_id` from all named channels, returning `(channel, remaining_total)`
    /// in subscription order down to `pattern_count`.
    pub fn unsubscribe_all(&self, client_id: u64) -> Vec<(Bytes, usize)> {
        let mut st = self.state.borrow_mut();
        let (removed_channels, pattern_count) = match st.clients.get_mut(&client_id) {
            Some(client) => {
                client.channel_set.clear();
                (std::mem::take(&mut client.channels), client.patterns.len())
            }
            None => (Vec::new(), 0),
        };

        let mut acks = Vec::with_capacity(removed_channels.len());
        let mut remaining = removed_channels.len() + pattern_count;

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

    /// Subscribe `client_id` to glob `pattern`. Duplicate pattern subscriptions are idempotent.
    pub fn psubscribe(&self, client_id: u64, pattern: Bytes) -> usize {
        let mut st = self.state.borrow_mut();
        let Some(client) = st.clients.get_mut(&client_id) else {
            return 0;
        };
        let is_new = client.pattern_set.insert(pattern.clone());
        if is_new {
            client.patterns.push(pattern.clone());
        }
        let total = client.total_subscriptions();
        if is_new {
            st.patterns.entry(pattern).or_default().insert(client_id);
        }
        total
    }

    /// Unsubscribe `client_id` from a single glob `pattern`. Returns remaining total count.
    pub fn punsubscribe(&self, client_id: u64, pattern: &Bytes) -> usize {
        let mut st = self.state.borrow_mut();
        let (removed, remaining) = match st.clients.get_mut(&client_id) {
            Some(client) => {
                let removed = client.pattern_set.remove(pattern);
                if removed {
                    client.patterns.retain(|p| p != pattern);
                }
                (removed, client.total_subscriptions())
            }
            None => (false, 0),
        };

        if removed {
            let mut empty = false;
            if let Some(subs) = st.patterns.get_mut(pattern) {
                subs.remove(&client_id);
                empty = subs.is_empty();
            }
            if empty {
                st.patterns.remove(pattern);
            }
        }

        remaining
    }

    /// Unsubscribe `client_id` from all patterns, returning `(pattern, remaining_total)`
    /// in subscription order down to `channel_count`.
    pub fn punsubscribe_all(&self, client_id: u64) -> Vec<(Bytes, usize)> {
        let mut st = self.state.borrow_mut();
        let (removed_patterns, channel_count) = match st.clients.get_mut(&client_id) {
            Some(client) => {
                client.pattern_set.clear();
                (std::mem::take(&mut client.patterns), client.channels.len())
            }
            None => (Vec::new(), 0),
        };

        let mut acks = Vec::with_capacity(removed_patterns.len());
        let mut remaining = removed_patterns.len() + channel_count;

        for pat in removed_patterns {
            remaining -= 1;
            let mut empty = false;
            if let Some(subs) = st.patterns.get_mut(&pat) {
                subs.remove(&client_id);
                empty = subs.is_empty();
            }
            if empty {
                st.patterns.remove(&pat);
            }
            acks.push((pat, remaining));
        }

        acks
    }

    /// Unsubscribe `client_id` from all named channels and patterns (used by `RESET` and `QUIT`).
    pub fn unsubscribe_everything(&self, client_id: u64) {
        let _ = self.unsubscribe_all(client_id);
        let _ = self.punsubscribe_all(client_id);
    }

    /// Total active subscriptions (named channels + patterns) for `client_id`.
    pub fn subscription_count(&self, client_id: u64) -> usize {
        self.state
            .borrow()
            .clients
            .get(&client_id)
            .map(ClientPubSub::total_subscriptions)
            .unwrap_or(0)
    }

    pub fn channel_subscription_count(&self, client_id: u64) -> usize {
        self.state
            .borrow()
            .clients
            .get(&client_id)
            .map(|c| c.channels.len())
            .unwrap_or(0)
    }

    pub fn pattern_subscription_count(&self, client_id: u64) -> usize {
        self.state
            .borrow()
            .clients
            .get(&client_id)
            .map(|c| c.patterns.len())
            .unwrap_or(0)
    }

    /// Publish `payload` to all live channel and matching pattern subscribers on `channel`.
    /// Returns the exact total number of deliveries made.
    /// Subscribers whose bounded queue is full or closed are evicted cleanly.
    pub fn publish(&self, channel: &Bytes, payload: &Bytes) -> i64 {
        let mut dead_clients: HashSet<u64> = HashSet::new();
        let mut delivered: i64 = 0;

        {
            let st = self.state.borrow();

            // 1. Named channel subscribers
            if let Some(subs) = st.channels.get(channel) {
                for &cid in subs {
                    if let Some(client) = st.clients.get(&cid) {
                        let msg = PubSubMessage::Channel {
                            channel: channel.clone(),
                            payload: payload.clone(),
                        };
                        if client.sender.try_send(msg).is_ok() {
                            delivered += 1;
                        } else {
                            dead_clients.insert(cid);
                        }
                    } else {
                        dead_clients.insert(cid);
                    }
                }
            }

            // 2. Glob pattern subscribers
            if !st.patterns.is_empty() {
                let channel_str = String::from_utf8_lossy(channel);
                for (pat, subs) in &st.patterns {
                    let pat_str = String::from_utf8_lossy(pat);
                    if !crate::commands::keys::glob_match(&pat_str, &channel_str) {
                        continue;
                    }
                    for &cid in subs {
                        if dead_clients.contains(&cid) {
                            continue;
                        }
                        if let Some(client) = st.clients.get(&cid) {
                            let msg = PubSubMessage::Pattern {
                                pattern: pat.clone(),
                                channel: channel.clone(),
                                payload: payload.clone(),
                            };
                            if client.sender.try_send(msg).is_ok() {
                                delivered += 1;
                            } else {
                                dead_clients.insert(cid);
                            }
                        } else {
                            dead_clients.insert(cid);
                        }
                    }
                }
            }
        }

        for cid in dead_clients {
            self.remove_client(cid);
        }

        delivered
    }

    /// Remove a disconnected or slow-evicted client and prune all of its subscriptions.
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
            for pat in client.patterns {
                let mut empty = false;
                if let Some(subs) = st.patterns.get_mut(&pat) {
                    subs.remove(&client_id);
                    empty = subs.is_empty();
                }
                if empty {
                    st.patterns.remove(&pat);
                }
            }
        }
    }

    /// List active named channels (used by `PUBSUB CHANNELS [pattern]`).
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

    /// Total number of active pattern subscriptions across all clients (`PUBSUB NUMPAT`).
    pub fn numpat(&self) -> i64 {
        let st = self.state.borrow();
        st.patterns.values().map(|s| s.len() as i64).sum()
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
