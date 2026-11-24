use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use tokio::sync::Notify;

/// Lightweight single-threaded key notification hub for blocking commands
/// (`BZPOPMIN`, `BZPOPMAX`, `BLPOP`, `BRPOP`, `XREAD BLOCK`), scoped per `(db, key)`.
///
/// Runs exclusively on the background worker's `LocalSet` OS thread.
pub struct KeyNotifier {
    waiters: RefCell<HashMap<(i16, String), Rc<Notify>>>,
}

impl KeyNotifier {
    pub fn new() -> Self {
        Self {
            waiters: RefCell::new(HashMap::new()),
        }
    }

    /// Wake all tasks currently blocked waiting on `(db, key)`.
    #[allow(dead_code)]
    pub fn notify_key(&self, db: i16, key: &str) {
        if let Some(n) = self.waiters.borrow().get(&(db, key.to_string())) {
            n.notify_waiters();
        }
    }

    /// Wake tasks for every mutated `(db, key)` pair.
    pub fn notify_many(&self, keys: &[(i16, String)]) {
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

    /// Acquire or create the `Notify` handles for a set of watched keys in `db`.
    pub fn get_notifiers(&self, db: i16, keys: &[String]) -> Vec<Rc<Notify>> {
        let mut map = self.waiters.borrow_mut();
        keys.iter()
            .map(|k| {
                Rc::clone(
                    map.entry((db, k.clone()))
                        .or_insert_with(|| Rc::new(Notify::new())),
                )
            })
            .collect()
    }

    /// Clean up unused `Notify` entries whose only remaining reference is the map itself.
    pub fn gc_keys(&self, db: i16, keys: &[String]) {
        let mut map = self.waiters.borrow_mut();
        for k in keys {
            let key = (db, k.clone());
            if let Some(n) = map.get(&key) {
                if Rc::strong_count(n) <= 1 {
                    map.remove(&key);
                }
            }
        }
    }
}
