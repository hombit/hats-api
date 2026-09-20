//! Holding a generated answer for the moments a client spends reading it.
//!
//! **A parquet reader opens one url three times** — a size probe, the footer, then the
//! data — and each of those is a request with no memory of the last. Without somewhere to
//! keep the answer, one `compute()` over one partition runs the same query three times and
//! reads the same bytes from the store three times. Measured against Gaia DR3: three runs,
//! 27.6 MB read apiece, 83 MB moved to deliver a 27.6 MB answer.
//!
//! So an answer is kept for a few moments after it is made, and the reads that follow are
//! slices of it. That is the whole of the ambition — it is not a cache of a catalog, or of
//! anything a second caller is likely to ask for. `[limits] query_cache_seconds` is how
//! long, and `0` turns it off.
//!
//! **Time is the only validator, which is what keeps the window short.** The key is the
//! request's own path and query string; nothing here asks the store whether the object has
//! changed, because that would be the round trip the cache exists to avoid. A file replaced
//! while a reader is part-way through it is the one case this gets wrong, and it is a case
//! a ranged read of that file has anyway: the reader is already assuming the bytes under it
//! hold still. What the TTL buys is that the assumption is bounded and stated.
//!
//! **Bounded in bytes as well as in time.** `[limits] max_query_cache_bytes` caps what is
//! held at once; an answer too large for the cap is never stored, and the oldest go first
//! when a new one does not fit. Without that, a service answering large queries would hold
//! every one of them for the whole window.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::app::answer::Counts;

/// Which answer this is: the url that asked for it, and nothing else.
///
/// The query string carries the projection, the predicate and the format, so two requests
/// with the same key are two requests for the same bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Asked {
    path: String,
    query: String,
}

impl Asked {
    pub fn new(path: &str, query: Option<&str>) -> Self {
        Self {
            path: path.to_owned(),
            query: query.unwrap_or_default().to_owned(),
        }
    }
}

/// One answer, and what it cost to make — so a request served from here reports the work
/// the answer took rather than the none this request did.
#[derive(Debug, Clone)]
pub struct Held {
    pub body: Bytes,
    pub counts: Counts,
}

#[derive(Debug)]
struct Entry {
    held: Held,
    made: Instant,
}

/// The answers held right now, and the two bounds on them.
#[derive(Debug)]
pub struct Answers {
    ttl: Duration,
    capacity: u64,
    kept: Mutex<Kept>,
}

#[derive(Debug, Default)]
struct Kept {
    entries: HashMap<Asked, Entry>,
    /// Oldest first, which is the order they are dropped in.
    order: VecDeque<Asked>,
    bytes: u64,
}

impl Answers {
    pub fn new(seconds: u64, capacity: u64) -> Self {
        Self {
            ttl: Duration::from_secs(seconds),
            capacity,
            kept: Mutex::new(Kept::default()),
        }
    }

    /// Whether anything is kept at all. `query_cache_seconds = 0` is off, and off means
    /// the lookups and the clones do not happen rather than that they always miss.
    pub fn on(&self) -> bool {
        !self.ttl.is_zero() && self.capacity > 0
    }

    /// The answer to this request, if one was made recently enough.
    pub fn get(&self, asked: &Asked) -> Option<Held> {
        if !self.on() {
            return None;
        }
        let mut kept = self.kept.lock().ok()?;
        let entry = kept.entries.get(asked)?;
        // Expired entries are dropped on the way past rather than swept: a key nobody asks
        // for again costs nothing until the next insert walks it off the front.
        if entry.made.elapsed() >= self.ttl {
            let bytes = entry.held.body.len() as u64;
            kept.entries.remove(asked);
            kept.bytes = kept.bytes.saturating_sub(bytes);
            return None;
        }
        Some(entry.held.clone())
    }

    /// Keep this answer, unless it is larger than everything the cache may hold.
    pub fn insert(&self, asked: Asked, held: Held) {
        if !self.on() {
            return;
        }
        let size = held.body.len() as u64;
        if size > self.capacity {
            return;
        }
        let Ok(mut kept) = self.kept.lock() else {
            return;
        };
        kept.forget_expired(self.ttl);
        while kept.bytes + size > self.capacity {
            if !kept.forget_oldest() {
                break;
            }
        }
        if let Some(previous) = kept.entries.insert(
            asked.clone(),
            Entry {
                held,
                made: Instant::now(),
            },
        ) {
            kept.bytes = kept.bytes.saturating_sub(previous.held.body.len() as u64);
            kept.order.retain(|key| key != &asked);
        }
        kept.order.push_back(asked);
        kept.bytes += size;
    }
}

impl Kept {
    fn forget_expired(&mut self, ttl: Duration) {
        while let Some(key) = self.order.front() {
            match self.entries.get(key) {
                // Already dropped by a `get`, so only the order needs tidying.
                None => {
                    self.order.pop_front();
                }
                Some(entry) if entry.made.elapsed() >= ttl => {
                    self.forget_oldest();
                }
                Some(_) => break,
            }
        }
    }

    /// Whether there was one to drop.
    fn forget_oldest(&mut self) -> bool {
        let Some(key) = self.order.pop_front() else {
            return false;
        };
        if let Some(entry) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(entry.held.body.len() as u64);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn held(size: usize) -> Held {
        Held {
            body: Bytes::from(vec![0_u8; size]),
            counts: Counts {
                num_rows: 1,
                data_bytes_read: 2,
            },
        }
    }

    fn asked(query: &str) -> Asked {
        Asked::new("/part0.parquet", Some(query))
    }

    #[test]
    fn an_answer_is_held_for_the_next_request() {
        let answers = Answers::new(240, 1024);
        answers.insert(asked("columns=a"), held(10));

        let found = answers.get(&asked("columns=a")).expect("nothing held");
        assert_eq!(found.body.len(), 10);
        assert_eq!(found.counts.data_bytes_read, 2);
        // A different question is a different answer, however alike the two look.
        assert!(answers.get(&asked("columns=b")).is_none());
    }

    /// The setting an operator turns it off with, and off is off rather than a cache that
    /// always misses: nothing is held, so nothing goes stale and nothing is copied.
    #[test]
    fn zero_seconds_holds_nothing() {
        let answers = Answers::new(0, 1024);
        answers.insert(asked("columns=a"), held(10));

        assert!(!answers.on());
        assert!(answers.get(&asked("columns=a")).is_none());
    }

    #[test]
    fn an_answer_is_let_go_of_when_it_expires() {
        let answers = Answers::new(1, 1024);
        answers.insert(asked("columns=a"), held(10));
        assert!(answers.get(&asked("columns=a")).is_some());

        std::thread::sleep(Duration::from_millis(1100));
        assert!(answers.get(&asked("columns=a")).is_none());
    }

    /// The bound that stops a run of large answers holding every one of them.
    #[test]
    fn the_oldest_go_when_the_cap_is_reached() {
        let answers = Answers::new(240, 100);
        answers.insert(asked("columns=a"), held(60));
        answers.insert(asked("columns=b"), held(60));

        assert!(
            answers.get(&asked("columns=a")).is_none(),
            "the oldest stayed"
        );
        assert!(answers.get(&asked("columns=b")).is_some());
    }

    #[test]
    fn an_answer_larger_than_the_cap_is_not_held() {
        let answers = Answers::new(240, 100);
        answers.insert(asked("columns=a"), held(200));

        assert!(answers.get(&asked("columns=a")).is_none());
    }
}
