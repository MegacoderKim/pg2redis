use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use tokio::sync::Notify;

/// Lightweight single-threaded key notification hub for blocking commands
/// (`BZPOPMIN`, `BZPOPMAX`, `BLPOP`, `BRPOP`, `XREAD BLOCK`).
///
/// Runs exclusively on the background worker's `LocalSet` OS thread.
pub struct KeyNotifier {
    waiters: RefCell<HashMap<String, Rc<Notify>>>,
}

impl KeyNotifier {
    pub fn new() -> Self {
        Self {
            waiters: RefCell::new(HashMap::new()),
        }
    }

    /// Wake all tasks currently blocked waiting on `key`.
    #[allow(dead_code)]
    pub fn notify_key(&self, key: &str) {
        if let Some(n) = self.waiters.borrow().get(key) {
            n.notify_waiters();
        }
    }

    /// Wake tasks for every mutated key in `keys`.
    pub fn notify_many(&self, keys: &[String]) {
        if keys.is_empty() {
            return;
        }
        let map = self.waiters.borrow();
        for k in keys {
            if let Some(n) = map.get(k) {
                n.notify_waiters();
            }
        }
    }

    /// Acquire or create the `Notify` handles for a set of watched keys.
    pub fn get_notifiers(&self, keys: &[String]) -> Vec<Rc<Notify>> {
        let mut map = self.waiters.borrow_mut();
        keys.iter()
            .map(|k| {
                Rc::clone(
                    map.entry(k.clone())
                        .or_insert_with(|| Rc::new(Notify::new())),
                )
            })
            .collect()
    }

    /// Clean up unused `Notify` entries whose only remaining reference is the map itself.
    pub fn gc_keys(&self, keys: &[String]) {
        let mut map = self.waiters.borrow_mut();
        for k in keys {
            if let Some(n) = map.get(k) {
                if Rc::strong_count(n) <= 1 {
                    map.remove(k);
                }
            }
        }
    }
}
