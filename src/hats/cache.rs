//! What this process remembers about the catalogs it has read, and for how long.
//!
//! **One entry per part of a catalog, not one per catalog.** The properties, the partition
//! list, the schema, the files inside each directory partition, the example position and the
//! layout of each of the collection's index catalogs are each an entry, filled when something
//! first asks for it and weighed when it is filled. So a part the budget cannot hold — the file names of a catalog of twelve thousand directory
//! partitions — is let go of on its own, and the catalog's properties stay.
//!
//! **Keyed by the catalog's url and the options that built its store.** The options are a
//! [`Fingerprint`], never the options themselves, so a credential is part of what an entry is
//! *found* by and never part of what one *holds*. Nothing kept here holds a store either: a
//! store is built from credentials, and every part is filled through the store of the request
//! that asked for it. Two requests with the same key reach the same bytes the same way, so
//! either may fill a part the other reads.
//!
//! **Every part belongs to one reading of the catalog's properties.** The properties are the
//! anchor, and reading them draws a generation that is part of every other entry's key. When
//! the anchor expires or is evicted, the next request reads the properties again under a new
//! generation, and every part filled under the old one becomes unreachable at once. Without
//! that, a partition list read before a catalog was republished could be paired with a file
//! listing read after it — an answer neither version of the catalog gives. For the same
//! reason a part never outlives its anchor: it expires when the anchor does.
//!
//! **A failed read is never kept.** A slot is a `OnceCell` that stores only success, so
//! concurrent requests for one part wait for one read rather than each making their own, and
//! if it fails each of them gets its own error and the next request tries again. Sharing one
//! error between them is not possible without losing what `ApiError::from_mount` needs to see.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use futures::future::BoxFuture;
use moka::Expiry;
use moka::sync::Cache;
use tokio::sync::OnceCell;

use crate::error::ApiError;
use crate::storage::{Fingerprint, RemoteDir};

use super::catalog::Described;
use super::index::IndexLayout;
use super::partitions::{HatsPartition, HatsPartitionList};

/// How long what is read from one catalog is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifetime {
    /// Not kept past the request that read it: `0`.
    Off,
    For(Duration),
    /// Kept until evicted for room: `inf`, for a catalog that never changes once published.
    Forever,
}

impl Lifetime {
    /// A number of seconds as the config writes it, where `inf` is a TOML float.
    pub fn from_seconds(seconds: f64) -> Result<Self, String> {
        if seconds.is_nan() || seconds < 0.0 {
            return Err(format!(
                "{seconds} is not a number of seconds; write 0 to keep nothing, or inf to \
                 keep a catalog until it is evicted"
            ));
        }
        if seconds == 0.0 {
            return Ok(Self::Off);
        }
        if seconds.is_infinite() {
            return Ok(Self::Forever);
        }
        Duration::try_from_secs_f64(seconds)
            .map(Self::For)
            .map_err(|error| format!("{seconds} seconds cannot be kept: {error}; write inf"))
    }

    fn deadline(self, now: Instant) -> Option<Instant> {
        match self {
            Self::Off => Some(now),
            // Past what an `Instant` can hold is as good as forever.
            Self::For(duration) => now.checked_add(duration),
            Self::Forever => None,
        }
    }
}

impl<'de> serde::Deserialize<'de> for Lifetime {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let seconds = f64::deserialize(deserializer)?;
        Self::from_seconds(seconds).map_err(serde::de::Error::custom)
    }
}

/// Every catalog part this process holds, under one byte budget.
///
/// Cheap to clone: the entries are shared.
#[derive(Clone)]
pub struct Catalogs {
    shared: Arc<Shared>,
}

struct Shared {
    slots: Cache<Key, Arc<Slot>>,
    /// Where the next anchor's generation comes from. Process-wide rather than per catalog,
    /// so a generation is never reused under a key whose old entries are still resident.
    generations: AtomicU64,
}

impl std::fmt::Debug for Catalogs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalogs")
            .field("entries", &self.shared.slots.entry_count())
            .field("bytes", &self.shared.slots.weighted_size())
            .finish()
    }
}

impl Catalogs {
    /// A cache holding at most `max_bytes` of catalog parts, by their estimated weight.
    pub fn new(max_bytes: u64) -> Self {
        let slots = Cache::builder()
            .max_capacity(max_bytes)
            .weigher(|_: &Key, slot: &Arc<Slot>| {
                u32::try_from(entry_weight(slot)).unwrap_or(u32::MAX)
            })
            .expire_after(Deadline)
            .build();
        Self {
            shared: Arc::new(Shared {
                slots,
                generations: AtomicU64::new(1),
            }),
        }
    }

    /// The budget, in the same estimated bytes an entry is weighed in.
    pub fn max_bytes(&self) -> u64 {
        self.shared
            .slots
            .policy()
            .max_capacity()
            .unwrap_or(u64::MAX)
    }

    /// The cache as one catalog sees it: this cache, and how long that catalog's parts live.
    pub fn with_lifetime(&self, lifetime: Lifetime) -> CatalogCache {
        CatalogCache {
            catalogs: self.clone(),
            lifetime,
            renew: false,
        }
    }
}

/// The cache and one catalog's lifetime in it — which depends on the mount the catalog is
/// under, and is the operator's rather than the caller's.
#[derive(Debug, Clone)]
pub struct CatalogCache {
    catalogs: Catalogs,
    lifetime: Lifetime,
    renew: bool,
}

impl CatalogCache {
    /// Nothing kept past the request, for a test or a caller with no cache to hand.
    pub fn off() -> Self {
        Catalogs::new(0).with_lifetime(Lifetime::Off)
    }

    pub fn lifetime(&self) -> Lifetime {
        self.lifetime
    }

    /// The budget every catalog's parts share, whatever their lifetimes.
    pub fn max_bytes(&self) -> u64 {
        self.catalogs.max_bytes()
    }

    /// The same cache, reading a catalog's properties again whether or not they are held.
    ///
    /// What is read replaces what was held only once the read has succeeded, and under a new
    /// generation, so every other part is read again with it. Until then requests go on being
    /// answered from what was there — which is what lets a catalog be refreshed before it
    /// expires rather than after, with no request finding it gone. A read that fails leaves
    /// the old one to expire on its own.
    #[must_use]
    pub fn renewed(&self) -> Self {
        Self {
            renew: true,
            ..self.clone()
        }
    }

    /// Where one catalog's parts are held, and the anchor they all belong to.
    pub(super) fn slots(&self, dir: &RemoteDir) -> Slots {
        match self.lifetime {
            Lifetime::Off => Slots::Local(Mutex::default()),
            lifetime => Slots::Shared {
                catalogs: self.catalogs.clone(),
                url: Arc::from(dir.url.as_str()),
                built_with: dir.built_with(),
                lifetime,
                renew: self.renew,

                anchor: std::sync::OnceLock::new(),
                held: Mutex::default(),
            },
        }
    }
}

/// Which part of a catalog an entry is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Part {
    /// The properties, and the collection they were reached through.
    Anchor,
    Partitions,
    /// `dataset/_common_metadata`'s schema, or its absence.
    CommonSchema,
    /// The schema a statement is planned against: `_common_metadata`'s, else a partition's.
    Schema,
    /// The names inside one directory partition.
    Files {
        order: u8,
        pixel: u64,
    },
    /// A position the catalog holds a row at.
    Position,
    /// The layout of one of the collection's index catalogs, by its place in `all_indexes`.
    Index {
        at: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    url: Arc<str>,
    built_with: Fingerprint,
    /// Zero for the anchor, which is what draws the generation the others carry.
    generation: u64,
    part: Part,
}

/// What a part holds once read.
#[derive(Debug)]
pub(super) enum Value {
    Anchor(Arc<Described>),
    Partitions(Arc<HatsPartitionList>),
    Schema(Option<SchemaRef>),
    Files(Arc<[String]>),
    Position((f64, f64)),
    Index(Arc<IndexLayout>),
}

impl Value {
    /// An estimate of the bytes this holds, which is what the budget is spent in. Only has to
    /// be right in proportion: a partition list is weighed by its cells and a schema by its
    /// fields, and neither is weighed to the allocator's byte.
    fn weight(&self) -> u64 {
        let size = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        match self {
            Self::Anchor(described) => described.weight(),
            Self::Partitions(list) => {
                size(list.len()).saturating_mul(size(size_of::<HatsPartition>()))
            }
            Self::Schema(schema) => schema.as_ref().map_or(0, |schema| {
                schema
                    .fields()
                    .iter()
                    .map(|field| field_weight(field))
                    .sum()
            }),
            Self::Files(names) => names
                .iter()
                .map(|name| size(name.len()).saturating_add(size(size_of::<String>())))
                .sum(),
            Self::Position(_) => 16,
            Self::Index(layout) => layout.weight(),
        }
    }
}

fn field_weight(field: &Field) -> u64 {
    let size = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    let own = size(field.name().len()).saturating_add(64).saturating_add(
        field
            .metadata()
            .iter()
            .map(|(key, value)| size(key.len() + value.len()))
            .sum(),
    );
    let nested: u64 = match field.data_type() {
        DataType::Struct(fields) => fields.iter().map(|field| field_weight(field)).sum(),
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::FixedSizeList(inner, _)
        | DataType::Map(inner, _) => field_weight(inner),
        _ => 0,
    };
    own.saturating_add(nested)
}

/// What an entry costs against the budget: its value and what holding it costs besides.
fn entry_weight(slot: &Slot) -> u64 {
    KEY_WEIGHT.saturating_add(slot.weight())
}

/// What an entry costs beyond its value: the key, the slot, the cache's own bookkeeping. The
/// url is shared between a catalog's entries, so it is not counted once per entry.
const KEY_WEIGHT: u64 = 128;

/// One part: its value once read, and when it stops being true.
#[derive(Debug)]
pub(super) struct Slot {
    value: OnceCell<Value>,
    /// Fixed when the slot is made. For the anchor that is now plus the lifetime; for every
    /// other part it is the anchor's, so nothing outlives the reading it belongs to.
    expires: Option<Instant>,
    /// The anchor's generation, drawn when its slot was made. Unused on the other parts.
    generation: u64,
}

impl Slot {
    fn new(expires: Option<Instant>, generation: u64) -> Self {
        Self {
            value: OnceCell::new(),
            expires,
            generation,
        }
    }

    fn weight(&self) -> u64 {
        self.value.get().map_or(0, Value::weight)
    }

    pub(super) fn value(&self) -> Option<&Value> {
        self.value.get()
    }
}

/// Each entry expires at its slot's own deadline, whenever it is inserted or re-inserted.
struct Deadline;

impl Deadline {
    fn left(slot: &Slot, now: Instant) -> Option<Duration> {
        slot.expires
            .map(|expires| expires.saturating_duration_since(now))
    }
}

impl Expiry<Key, Arc<Slot>> for Deadline {
    fn expire_after_create(&self, _: &Key, slot: &Arc<Slot>, now: Instant) -> Option<Duration> {
        Self::left(slot, now)
    }

    /// A slot is re-inserted once it is filled, so that the weigher sees what it now holds.
    /// That must not move its deadline, or a part filled late would outlive its anchor.
    fn expire_after_update(
        &self,
        _: &Key,
        slot: &Arc<Slot>,
        now: Instant,
        _: Option<Duration>,
    ) -> Option<Duration> {
        Self::left(slot, now)
    }
}

/// Where one catalog's parts are, as the handle that reads them sees it.
#[derive(Debug)]
pub(super) enum Slots {
    /// Nothing is kept past the request: each part is read at most once by this handle and
    /// dropped with it.
    Local(Mutex<HashMap<Part, Arc<Slot>>>),
    Shared {
        catalogs: Catalogs,
        url: Arc<str>,
        built_with: Fingerprint,
        lifetime: Lifetime,
        /// Whether the anchor is read afresh rather than found — see [`CatalogCache::renewed`].
        renew: bool,
        /// The anchor's slot, once found, which is where every other part's generation and
        /// deadline come from.
        anchor: std::sync::OnceLock<Arc<Slot>>,
        /// Every part this handle has read or found, and what each weighs against the budget.
        held: Mutex<HashMap<Part, u64>>,
    },
}

impl Slots {
    /// The slot for a part, made if it is not there.
    fn slot(&self, part: Part) -> Result<(Arc<Slot>, Option<Key>), ApiError> {
        match self {
            Self::Local(slots) => {
                let mut slots = slots
                    .lock()
                    .map_err(|_| ApiError::internal("a catalog's parts were poisoned"))?;
                let slot = slots
                    .entry(part)
                    .or_insert_with(|| Arc::new(Slot::new(None, 0)));
                Ok((Arc::clone(slot), None))
            }
            Self::Shared {
                catalogs,
                url,
                built_with,
                lifetime,
                renew,
                anchor,
                ..
            } => {
                let key = |generation| Key {
                    url: Arc::clone(url),
                    built_with: *built_with,
                    generation,
                    part,
                };
                if part == Part::Anchor {
                    let key = key(0);
                    let fresh = || {
                        let generation =
                            catalogs.shared.generations.fetch_add(1, Ordering::Relaxed);
                        Arc::new(Slot::new(lifetime.deadline(Instant::now()), generation))
                    };
                    // Renewing makes a slot the cache does not hold yet: `filled` puts it in,
                    // over the old one, only once it holds something.
                    let slot = match renew {
                        true => fresh(),
                        false => catalogs.shared.slots.get_with(key.clone(), fresh),
                    };
                    return Ok((slot, Some(key)));
                }
                let anchor = anchor.get().ok_or_else(|| {
                    ApiError::internal("a catalog part was read before its anchor")
                })?;
                let key = key(anchor.generation);
                let slot = catalogs
                    .shared
                    .slots
                    .get_with(key.clone(), || Arc::new(Slot::new(anchor.expires, 0)));
                Ok((slot, Some(key)))
            }
        }
    }

    /// Count a filled part towards what this handle holds.
    fn hold(&self, part: Part, slot: &Slot) -> Result<(), ApiError> {
        if let Self::Shared { held, .. } = self
            && slot.value.initialized()
        {
            held.lock()
                .map_err(|_| ApiError::internal("a catalog's parts were poisoned"))?
                .insert(part, entry_weight(slot));
        }
        Ok(())
    }

    /// What the parts this handle has read or found weigh against the cache's budget, each
    /// counted once however often it was asked for. Nothing, where nothing is kept.
    ///
    /// A part read only inside another's fill — `_common_metadata`'s schema, for the schema —
    /// is counted by the handle that read it, and not by one that found the outer part already
    /// filled. So this is the whole of a catalog only for a handle that read it from nothing:
    /// a first reading, or a renewal.
    pub(super) fn weight(&self) -> Result<u64, ApiError> {
        match self {
            Self::Local(_) => Ok(0),
            Self::Shared { held, .. } => Ok(held
                .lock()
                .map_err(|_| ApiError::internal("a catalog's parts were poisoned"))?
                .values()
                .fold(0, |total, weight| total.saturating_add(*weight))),
        }
    }

    /// A part's value, read by `fill` where no request has read it yet.
    ///
    /// `fill` arrives boxed: one part's read asks for others — the schema for the partition
    /// list, then a partition's files — and each is a DataFusion read, so held inline they
    /// nest into a future larger than a thread's stack.
    pub(super) async fn filled(
        &self,
        part: Part,
        fill: BoxFuture<'_, Result<Value, ApiError>>,
    ) -> Result<Arc<Slot>, ApiError> {
        let (slot, key) = self.slot(part)?;
        // Whether it was read by this request or an earlier one, it is the anchor every other
        // part of this handle is keyed under.
        if part == Part::Anchor
            && let Self::Shared { anchor, .. } = self
        {
            let _ = anchor.set(Arc::clone(&slot));
        }
        if slot.value.initialized() {
            self.hold(part, &slot)?;
            return Ok(slot);
        }
        slot.value.get_or_try_init(|| fill).await?;
        if let (Self::Shared { catalogs, .. }, Some(key)) = (self, key) {
            // Weighed again now that it holds something: the weigher only runs on insertion,
            // and the slot went in empty.
            catalogs.shared.slots.insert(key, Arc::clone(&slot));
        }
        self.hold(part, &slot)?;
        Ok(slot)
    }

    /// A value read elsewhere — a walk of a whole dataset lists every partition at once — put
    /// where a later request for that part will find it. A part already filled keeps its
    /// value: the two were read from the same catalog under the same anchor.
    pub(super) fn offer(&self, part: Part, value: Value) -> Result<(), ApiError> {
        let (slot, key) = self.slot(part)?;
        if slot.value.set(value).is_ok()
            && let (Self::Shared { catalogs, .. }, Some(key)) = (self, key)
        {
            catalogs.shared.slots.insert(key, Arc::clone(&slot));
        }
        self.hold(part, &slot)
    }

    /// A part's value where it has been read already, reading nothing.
    pub(super) fn peek(&self, part: Part) -> Result<Option<Arc<Slot>>, ApiError> {
        let found = match self {
            Self::Local(slots) => slots
                .lock()
                .map_err(|_| ApiError::internal("a catalog's parts were poisoned"))?
                .get(&part)
                .cloned(),
            Self::Shared {
                catalogs,
                url,
                built_with,
                anchor,
                ..
            } => {
                let Some(anchor) = anchor.get() else {
                    return Ok(None);
                };
                catalogs.shared.slots.get(&Key {
                    url: Arc::clone(url),
                    built_with: *built_with,
                    generation: anchor.generation,
                    part,
                })
            }
        };
        Ok(found.filter(|slot| slot.value.initialized()))
    }
}

/// A slot whose value is not the part it was asked for, which only a bug here produces.
pub(super) fn mismatched(part: Part) -> ApiError {
    ApiError::internal(format!(
        "a cached catalog part {part:?} held the wrong kind of value"
    ))
}
