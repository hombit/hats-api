//! Opening a catalog: which of its own files describe it, where its partitions are, and what
//! a request needs to know about it before it reads a row.
//!
//! **Only the properties are read on opening.** They are what decides the url is a catalog at
//! all; everything else — the partition list, the files inside a directory partition, the
//! schema, a position to centre an example on — is read the first time something asks for
//! it, and held in the catalog's slots from then on. Whether those slots outlive the request
//! is [`CatalogCache`]'s decision; this module reads the same way either way.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::prelude::ParquetReadOptions;
use futures::{FutureExt, StreamExt, TryStreamExt, stream};

use crate::access::data::DataFiles;
use crate::error::ApiError;
use crate::storage::{RemoteDir, RemoteFile};

use super::cache::{CatalogCache, Part, Slot, Slots, Value, mismatched};
use super::index::{IndexLayout, Lookup};
use super::partitions::{
    self, COMMON_METADATA, DATA_THUMBNAIL, DATASET_DIR, HatsPartition, HatsPartitionList,
};
use super::properties::{self, Properties};

/// How many names one listing request brings back. S3 caps a page of `ListObjectsV2` at a
/// thousand keys and the other stores are the same order, so this is what a walk of a whole
/// dataset costs per thousand files — the number [`HatsCatalog::names`] weighs one request
/// per partition against.
const LISTING_PAGE: usize = 1_000;

/// How many partition listings are in flight at once.
///
/// Deliberately not `max_concurrent_partitions`, which bounds *reads*: a read pulls row groups
/// into memory, so a handful at a time is the point of it. A listing is a few kilobytes of
/// names and costs a round trip, so the two want opposite numbers. The count is small either
/// way — [`HatsCatalog::names`] only lists partition by partition when there are fewer of
/// them than a walk of the whole dataset would cost in pages.
const LISTING_CONCURRENCY: usize = 16;

/// An opened catalog, as one request reads it.
///
/// It holds the request's own store and never hands that to anything kept past the request:
/// a store is built from the caller's credentials, and what the slots keep is only what was
/// read through it.
#[derive(Debug)]
pub struct HatsCatalog {
    /// This catalog's own directory — below the collection, where one was followed — opened
    /// through this request's store.
    dir: RemoteDir,
    /// The collection's directory, where a collection was followed to reach this catalog:
    /// where its index catalogs are.
    collection_dir: Option<RemoteDir>,
    described: Arc<Described>,
    slots: Slots,
    max_metadata_bytes: u64,
}

/// What a catalog says about itself, which is what decides it is one.
#[derive(Debug)]
pub(super) struct Described {
    properties: Properties,
    collection: Option<Properties>,
    /// The path from the url the caller named down to this catalog, empty unless a
    /// collection was followed to get here.
    ///
    /// Everything that names a file to a caller is built from the url they wrote, so the hop
    /// a collection made has to be carried rather than dropped: a plan entry built as if the
    /// collection's own directory held the partitions names a path that is not there.
    within: String,
}

impl Described {
    pub(super) fn weight(&self) -> u64 {
        let within = u64::try_from(self.within.len()).unwrap_or(u64::MAX);
        self.properties
            .weight()
            .saturating_add(self.collection.as_ref().map_or(0, Properties::weight))
            .saturating_add(within)
    }
}

impl HatsCatalog {
    /// Read what a catalog says about itself, and nothing more.
    ///
    /// `hats.properties`, then the deprecated `properties`, then `collection.properties` —
    /// so a catalog is described by the first request and only a collection pays for the
    /// other two. A collection opens as the catalog it calls its primary table, which it
    /// may only name as a path inside itself.
    pub async fn open(
        dir: RemoteDir,
        cache: &CatalogCache,
        max_metadata_bytes: u64,
    ) -> Result<Self, ApiError> {
        let slots = cache.slots(&dir);
        let slot = slots.filled(Part::Anchor, describe(&dir).boxed()).await?;
        let Some(Value::Anchor(described)) = slot.value() else {
            return Err(mismatched(Part::Anchor));
        };
        let described = Arc::clone(described);
        let (dir, collection_dir) = match described.within.trim_end_matches('/') {
            "" => (dir, None),
            table => (dir.subdir(table)?, Some(dir)),
        };
        Ok(Self {
            dir,
            collection_dir,
            described,
            slots,
            max_metadata_bytes,
        })
    }

    pub fn dir(&self) -> &RemoteDir {
        &self.dir
    }

    /// The path from the url the caller named down to this catalog — empty for a catalog
    /// named directly, and the primary table's path for one reached through a collection.
    ///
    /// What it is for is building a name a caller can send back. Everything this service
    /// hands out is spelled in the caller's own url, and joining a path below the *catalog*
    /// onto the url of the *collection* skips exactly this hop.
    pub fn within(&self) -> &str {
        &self.described.within
    }

    /// The collection this catalog was reached through, if it was.
    ///
    /// What it carries beyond the primary table — `all_margins`, `default_margin`,
    /// `all_indexes` — is not read yet, and each of those is a catalog of its own.
    pub fn collection(&self) -> Option<&Properties> {
        self.described.collection.as_ref()
    }

    /// What the parts of this catalog read or found through this handle weigh against the
    /// cache's budget — the room this catalog, read this far, takes up in it.
    pub fn cached_weight(&self) -> Result<u64, ApiError> {
        self.slots.weight()
    }

    pub fn properties(&self) -> &Properties {
        &self.described.properties
    }

    /// The partitions, in HEALPix order.
    ///
    /// `partition_info.csv`, then `_metadata`, then a listing of `dataset/` — see
    /// [`partitions::discover`] for why that order.
    pub async fn partitions(&self) -> Result<Arc<HatsPartitionList>, ApiError> {
        let slot = self
            .slots
            .filled(
                Part::Partitions,
                async {
                    let list = partitions::discover(&self.dir, self.max_metadata_bytes).await?;
                    Ok(Value::Partitions(Arc::new(list)))
                }
                .boxed(),
            )
            .await?;
        match slot.value() {
            Some(Value::Partitions(list)) => Ok(Arc::clone(list)),
            _ => Err(mismatched(Part::Partitions)),
        }
    }

    /// Where a partition's rows are, which is one file or a directory of them.
    ///
    /// `hats_npix_suffix` decides which, and `/` — a directory — is what a catalog large
    /// enough to split a partition writes. It is not a rare shape: ZTF DR24's object
    /// catalog is one. So nothing may assume a partition is a single object.
    pub fn partition(&self, partition: &HatsPartition) -> Result<Partitioned, ApiError> {
        let suffix = self.properties().npix_suffix();
        let path = partition.path(suffix);
        match self.properties().partition_is_a_directory() {
            false => Ok(Partitioned::One(self.dir.child(&path)?)),
            true => Ok(Partitioned::Many(self.dir.subdir(&path)?)),
        }
    }

    /// The files to read for one partition.
    ///
    /// **Where `hats_npix_suffix` names a file, that name is the answer.** The path is
    /// constructed from the cell and the suffix, and nothing filters it — no listing, no
    /// `data`. A catalog writing its partitions as `.parq`, or as anything else it cares to
    /// name, reads on the strength of what it said about itself, whether or not that suffix
    /// is one the file server would answer a query on.
    ///
    /// A suffix of `/` is the only case with a question in it: the partition is a directory
    /// and the names inside it are the catalog's business rather than the format's. Those
    /// are matched against `data`, so a `_SUCCESS` marker or a checksum beside the parts is
    /// passed over the way the rest of this service passes over a name it does not read.
    ///
    /// **That case needs a listing, and not every backend has one.** An `http(s)://`
    /// catalog written this way cannot be read at all: the names inside a partition appear
    /// in none of the catalog's metadata, so there is nothing to derive them from.
    pub async fn files(
        &self,
        partition: &HatsPartition,
        data: &DataFiles,
    ) -> Result<Vec<RemoteFile>, ApiError> {
        match self.partition(partition)? {
            Partitioned::One(file) => Ok(vec![file]),
            Partitioned::Many(dir) => self
                .listed(partition, &dir)
                .await?
                .iter()
                .filter(|name| data.matches(basename(name)))
                .map(|name| dir.child(name))
                .collect(),
        }
    }

    /// The data files inside each of `partitions`, named relative to its directory and in
    /// the same order — for a catalog whose partitions are directories.
    ///
    /// **Two walks, and the cheaper is chosen by counting requests.** Listing each partition
    /// is one request apiece; listing the whole dataset is one per `LISTING_PAGE` files
    /// however many partitions that spans. So a region that reached a handful asks for exactly
    /// those, and a plan over a whole catalog reads the lot — thirteen requests against 12,485
    /// for ZTF DR24, which is the difference between a plan that answers and one nobody waits
    /// for. Both produce the same names, and a partition some earlier request listed costs
    /// neither, so only the ones still unread are counted.
    pub async fn names(
        &self,
        partitions: &[HatsPartition],
        data: &DataFiles,
    ) -> Result<Vec<Vec<String>>, ApiError> {
        if !self.properties().partition_is_a_directory() {
            return Err(ApiError::internal(
                "a partition's file names were asked of a catalog whose partitions are files",
            ));
        }
        let mut unread = 0;
        for partition in partitions {
            if self.slots.peek(files_part(partition))?.is_none() {
                unread += 1;
            }
        }
        // One file per partition is the least a dataset can hold, so this is a floor on the
        // pages a walk would cost. Erring low errs towards asking partition by partition,
        // which is the walk that reads nothing it was not asked for.
        let pages = self.partitions().await?.len().div_ceil(LISTING_PAGE);
        if unread > pages {
            self.walk_dataset().await?;
        }
        // Materialized before the stream: a closure handing back a future that borrows its
        // argument owes a higher-ranked bound the compiler will not infer here. Nothing is
        // listed until the stream is polled, and `buffered` yields by position, so the names
        // come back in `partitions`' order whatever order the listings land in.
        let listings = partitions
            .iter()
            .map(|partition| async move {
                let Partitioned::Many(dir) = self.partition(partition)? else {
                    return Ok(Vec::new());
                };
                Ok::<_, ApiError>(
                    self.listed(partition, &dir)
                        .await?
                        .iter()
                        .filter(|name| data.matches(basename(name)))
                        .cloned()
                        .collect(),
                )
            })
            .collect::<Vec<_>>();
        stream::iter(listings)
            .buffered(LISTING_CONCURRENCY)
            .try_collect()
            .await
    }

    /// Every name inside one directory partition, as the store lists it.
    async fn listed(
        &self,
        partition: &HatsPartition,
        dir: &RemoteDir,
    ) -> Result<Arc<[String]>, ApiError> {
        let part = files_part(partition);
        let slot = self
            .slots
            .filled(
                part,
                async {
                    let entries = dir.list("").await?;
                    Ok(Value::Files(
                        entries.into_iter().map(|entry| entry.name).collect(),
                    ))
                }
                .boxed(),
            )
            .await?;
        names_in(&slot, part)
    }

    /// One recursive listing of the whole dataset, put where each partition's names would
    /// have gone had it been listed on its own.
    ///
    /// A listing is recursive and paginated by the store, so this is one walk rather than one
    /// request: every partition's files arrive in it, and a partition with none gets an empty
    /// list rather than a listing of its own later. Names come back relative to the dataset
    /// directory, and `Norder=…/Dir=…/Npix=…` is the three segments a partition's own path has
    /// once that prefix is off, so the two meet as strings and neither becomes a url.
    async fn walk_dataset(&self) -> Result<(), ApiError> {
        let listing = self.dir.list(DATASET_DIR).await?;
        let mut found: HashMap<&str, Vec<String>> = HashMap::new();
        for entry in &listing {
            let mut segments = entry.name.splitn(4, '/');
            let (Some(order), Some(group), Some(pixel), Some(name)) = (
                segments.next(),
                segments.next(),
                segments.next(),
                segments.next(),
            ) else {
                continue;
            };
            let directory = &entry.name[..order.len() + group.len() + pixel.len() + 2];
            found.entry(directory).or_default().push(name.to_owned());
        }
        let prefix = format!("{DATASET_DIR}/");
        let suffix = self.properties().npix_suffix();
        for partition in self.partitions().await?.cells() {
            let path = partition.path(suffix);
            // `path` ends in the suffix, which for these catalogs is the `/` the listing's
            // own split took off.
            let key = path
                .strip_prefix(&prefix)
                .unwrap_or(&path)
                .trim_end_matches('/');
            let names = found.remove(key).unwrap_or_default();
            self.slots
                .offer(files_part(partition), Value::Files(names.into()))?;
        }
        Ok(())
    }

    /// `dataset/_common_metadata`'s schema: every partition's columns, and no rows. `None`
    /// where the catalog has no such file, or it cannot be read.
    pub async fn common_schema(&self) -> Option<SchemaRef> {
        let slot = self
            .slots
            .filled(
                Part::CommonSchema,
                async {
                    // Asked first, so that the absence — a fact about the catalog — is kept, and a
                    // failure to read one that is there is not.
                    if self.dir.meta(COMMON_METADATA).await?.is_none() {
                        return Ok(Value::Schema(None));
                    }
                    let file = self.dir.child(COMMON_METADATA)?;
                    Ok(Value::Schema(Some(read_schema(&file).await?)))
                }
                .boxed(),
            )
            .await
            .ok()?;
        match slot.value() {
            Some(Value::Schema(schema)) => schema.clone(),
            _ => None,
        }
    }

    /// The catalog's schema, which a statement is checked against before anything is read.
    ///
    /// `dataset/_common_metadata` first: it is the schema and no rows, so it is one small
    /// `GET` and it describes every partition rather than the one that answered. A catalog
    /// that has not got it falls back to the first partition's footer, which is the more
    /// expensive of the two and second for that reason.
    pub async fn schema(&self, data: &DataFiles) -> Result<SchemaRef, ApiError> {
        let slot = self
            .slots
            .filled(
                Part::Schema,
                async {
                    if let Some(schema) = self.common_schema().await {
                        return Ok(Value::Schema(Some(schema)));
                    }
                    let partitions = self.partitions().await?;
                    let Some(first) = partitions.cells().first() else {
                        return Err(ApiError::bad_request(
                            "this catalog has no partitions, so nothing says what columns it has",
                        ));
                    };
                    let files = self.files(first, data).await?;
                    let Some(file) = files.first() else {
                        return Err(ApiError::bad_request(
                            "this catalog's first partition holds no data file, so nothing says \
                         what columns it has",
                        ));
                    };
                    Ok(Value::Schema(Some(read_schema(file).await?)))
                }
                .boxed(),
            )
            .await?;
        match slot.value() {
            Some(Value::Schema(Some(schema))) => Ok(Arc::clone(schema)),
            _ => Err(mismatched(Part::Schema)),
        }
    }

    /// A position the catalog really holds a row at, as `(ra, dec)` in degrees, read out of
    /// one of its own rows.
    ///
    /// **A cone has to be centred on something that is there, and only a row says where that
    /// is.** Everything cheaper is a guess that fails on some real catalog: a partition's
    /// centre is empty wherever the data fills a corner of its cell, which is every catalog
    /// covering a patch of sky rather than the whole of it. `hats` reaches the same conclusion
    /// in `io/summary_file.py`, which takes the `ra` and `dec` of an example row.
    ///
    /// `data_thumbnail.parquet` first, which is what `hats` writes at a catalog's root for
    /// exactly this — a handful of rows, so reading it is one small `GET`. A catalog without
    /// one costs the first row group of two columns of its deepest partition instead.
    ///
    /// `None` where the catalog names no position columns or neither read gives a row; only a
    /// position found is kept, so a read that failed on the way is tried again next time.
    pub async fn example_position(&self, data: &DataFiles) -> Option<(f64, f64)> {
        let (ra, dec) = self.properties().coordinate_columns()?;
        let slot = self
            .slots
            .filled(
                Part::Position,
                async {
                    self.find_position(data, ra, dec)
                        .await
                        .map(Value::Position)
                        .ok_or_else(|| {
                            ApiError::not_found("no row of this catalog gives a position")
                        })
                }
                .boxed(),
            )
            .await
            .ok()?;
        match slot.value() {
            Some(Value::Position(position)) => Some(*position),
            _ => None,
        }
    }

    /// The partitions holding any of `values` of `column`, by the collection's index on it, and
    /// what asking cost.
    ///
    /// `None` wherever the index is not used: there is none to ask — a catalog named outside its
    /// collection, or a column no index covers — it cannot be read, which is logged, or reading
    /// it could cost more than `max_bytes`. Every partition is then a candidate, as it is for a
    /// catalog with no index at all. The same rows come back either way; what the index changes
    /// is how many partitions are opened.
    ///
    /// `healpix` is the table's HEALPix column, whose values of the rows found come back too
    /// where the index carries it.
    pub(crate) async fn indexed_partitions(
        &self,
        column: &str,
        values: &HashSet<ScalarValue>,
        data: &DataFiles,
        max_bytes: u64,
        healpix: Option<&str>,
    ) -> Option<Lookup> {
        let (collection, root) = (self.collection()?, self.collection_dir.as_ref()?);
        let listed = collection
            .indexes()
            .inspect_err(|error| tracing::warn!(%error, "cannot read a collection's indexes"))
            .ok()?;
        let (at, name) = listed
            .iter()
            .enumerate()
            .find_map(|(at, (indexed, name))| (*indexed == column).then_some((at, *name)))?;
        let looked_up = async {
            let dir = root.subdir(inside_collection(name, "all_indexes")?)?;
            let part = Part::Index { at };
            let slot = self
                .slots
                .filled(
                    part,
                    async {
                        let layout =
                            IndexLayout::read(&dir, column, data, self.max_metadata_bytes).await?;
                        Ok(Value::Index(Arc::new(layout)))
                    }
                    .boxed(),
                )
                .await?;
            let Some(Value::Index(layout)) = slot.value() else {
                return Err(mismatched(part));
            };
            layout
                .partitions_for(&dir, values, max_bytes, healpix)
                .await
        };
        looked_up
            .await
            .inspect_err(|error| {
                tracing::warn!(%error, column, "cannot use a collection's index; reading every partition");
            })
            .ok()
            .flatten()
    }

    async fn find_position(&self, data: &DataFiles, ra: &str, dec: &str) -> Option<(f64, f64)> {
        if let Ok(thumbnail) = self.dir.child(DATA_THUMBNAIL)
            && let Some(found) = read_position(&thumbnail, ra, dec).await
        {
            return Some(found);
        }
        let partitions = self.partitions().await.ok()?;
        let files = self.files(partitions.deepest()?, data).await.ok()?;
        read_position(files.first()?, ra, dec).await
    }

    /// The coordinate columns and the HEALPix column, from the catalog's properties.
    ///
    /// This is what makes `ra_column` and `dec_column` optional against a catalog and
    /// required against a lone parquet file: a file says nothing about which of its columns
    /// are a position, and a catalog says exactly that.
    pub fn columns(&self) -> Result<Columns<'_>, ApiError> {
        let properties = self.properties();
        let (ra, dec) = properties.coordinate_columns().ok_or_else(|| {
            ApiError::bad_request(
                "this catalog does not name its position columns; send ra_column and \
                 dec_column",
            )
        })?;
        Ok(Columns {
            ra,
            dec,
            healpix: properties.healpix_column()?,
        })
    }
}

fn files_part(partition: &HatsPartition) -> Part {
    Part::Files {
        order: partition.order,
        pixel: partition.pixel,
    }
}

fn names_in(slot: &Slot, part: Part) -> Result<Arc<[String]>, ApiError> {
    match slot.value() {
        Some(Value::Files(names)) => Ok(Arc::clone(names)),
        _ => Err(mismatched(part)),
    }
}

/// The last segment of a name inside a partition, which is what `data` judges.
fn basename(name: &str) -> &str {
    name.rsplit('/').next().unwrap_or(name)
}

/// The properties, following a collection one hop down to its primary table.
async fn describe(dir: &RemoteDir) -> Result<Value, ApiError> {
    let described = match read_properties(dir).await? {
        Some(properties) => Described {
            properties,
            collection: None,
            within: String::new(),
        },
        None => {
            let collection = read_collection(dir).await?.ok_or_else(not_a_catalog)?;
            let table = primary_table(&collection)?.trim_end_matches('/').to_owned();
            let inside = dir.subdir(&table)?;
            let properties = read_properties(&inside).await?.ok_or_else(not_a_catalog)?;
            Described {
                properties,
                collection: Some(collection),
                within: format!("{table}/"),
            }
        }
    };
    Ok(Value::Anchor(Arc::new(described)))
}

/// The properties file, under either of its two names, preferred one first.
async fn read_properties(dir: &RemoteDir) -> Result<Option<Properties>, ApiError> {
    for name in properties::NAMES {
        if let Some(bytes) = dir.read_if_present(name).await? {
            return Properties::parse(&bytes).map(Some);
        }
    }
    Ok(None)
}

/// A 404 rather than a 400: the caller named a url this service can reach and there is no
/// catalog at it, which is the same thing a missing file is — and a directory that is not
/// one may simply be the wrong path.
fn not_a_catalog() -> ApiError {
    ApiError::not_found(format!(
        "this url is not a HATS catalog: it has no {} and no {}",
        properties::NAMES.join(", no "),
        properties::COLLECTION
    ))
}

/// A `collection.properties`, if this directory is a collection rather than a catalog.
async fn read_collection(dir: &RemoteDir) -> Result<Option<Properties>, ApiError> {
    match dir.read_if_present(properties::COLLECTION).await? {
        Some(bytes) => Properties::parse(&bytes).map(Some),
        None => Ok(None),
    }
}

/// The catalog a collection points at, **only when it points inside itself**.
///
/// `hats_primary_table_url` is a url-shaped key, and following one that is not a relative
/// path would make a file this service reads decide where it connects next: an absolute
/// path reaches anywhere the local rules allow, and a url with a scheme reaches a server no
/// endpoint rule ever named. Neither is a request the caller made — the caller named the
/// collection, and everything below it is what they thereby asked for.
///
/// So a collection is followed one hop, downwards, and anything else is refused with a
/// message saying the caller may name the catalog directly. That costs a collection whose
/// members are published apart from it, which is not a shape a collection is written in.
pub(super) fn primary_table(collection: &Properties) -> Result<&str, ApiError> {
    let named = collection.get("hats_primary_table_url").ok_or_else(|| {
        ApiError::bad_request(
            "this collection's hats_primary_table_url is missing; name the catalog's own url",
        )
    })?;
    inside_collection(named, "hats_primary_table_url")
}

/// A catalog a collection names by `key`, **only where it is a path inside the collection** —
/// see [`primary_table`] for why nothing else is followed. Its indexes are held to the same
/// rule as its primary table, being catalogs this service would otherwise connect to on a
/// file's say-so.
fn inside_collection<'a>(named: &'a str, key: &str) -> Result<&'a str, ApiError> {
    let refuse = |why: &str| {
        ApiError::bad_request(format!(
            "this collection's {key} {why}; name the catalog's own url"
        ))
    };
    if named.contains("://") || named.starts_with('/') || named.starts_with('\\') {
        return Err(refuse("is not a path inside the collection"));
    }
    // `RemoteDir::join` refuses `..` too, but the message it gives is about a name rather
    // than about a collection, and this is the one place the difference is worth saying.
    if named
        .split('/')
        .any(|segment| segment == ".." || segment.is_empty())
    {
        return Err(refuse("does not name a directory inside the collection"));
    }
    Ok(named.trim_end_matches('/'))
}

/// One parquet file's schema, with no rows read.
///
/// Inferred by DataFusion in a context of its own, the way every route here plans against a
/// file, so the schema kept is the one a scan of the file would be planned against.
async fn read_schema(file: &RemoteFile) -> Result<SchemaRef, ApiError> {
    let ctx = crate::engine::query::session_context(false);
    ctx.register_object_store(&file.base, Arc::clone(&file.store));
    // The extension filter is off for the reason it is off everywhere here: a HATS partition
    // may be named anything its `hats_npix_suffix` says, and `_common_metadata` has no
    // extension at all.
    let options = ParquetReadOptions {
        file_extension: "",
        ..Default::default()
    };
    let frame = ctx
        .read_parquet(file.url.as_str(), options)
        .await
        .map_err(|error| {
            ApiError::bad_request(format!("this catalog's columns could not be read: {error}"))
        })?;
    Ok(SchemaRef::from(frame.schema().as_arrow().clone()))
}

/// The two coordinates of one row of one parquet file.
///
/// The columns are named as `Column`s rather than parsed from text: a catalog's column may be
/// mixed-case or carry a character the parser would read as structure. `limit(1)` is what
/// keeps it to a row group.
async fn read_position(file: &RemoteFile, ra: &str, dec: &str) -> Option<(f64, f64)> {
    let ctx = crate::engine::query::session_context(false);
    ctx.register_object_store(&file.base, Arc::clone(&file.store));
    let options = ParquetReadOptions {
        // A HATS partition is named whatever `hats_npix_suffix` says, and the thumbnail is
        // read the same way for the same reason the schema is.
        file_extension: "",
        ..Default::default()
    };
    let named = |name: &str| Expr::Column(Column::new_unqualified(name.to_owned()));
    let batches = ctx
        .read_parquet(file.url.as_str(), options)
        .await
        .ok()?
        .select(vec![named(ra), named(dec)])
        .ok()?
        .limit(0, Some(1))
        .ok()?
        .collect()
        .await
        .ok()?;
    let batch = batches.iter().find(|batch| batch.num_rows() > 0)?;
    // Cast rather than match: ZTF DR24 writes its coordinates as `Float32`, and a catalog is
    // free to write them at any width.
    let value = |at: usize| -> Option<f64> {
        let column = cast(batch.column(at), &DataType::Float64).ok()?;
        let column = column.as_any().downcast_ref::<Float64Array>()?;
        column.is_valid(0).then(|| column.value(0))
    };
    let (ra, dec) = (value(0)?, value(1)?);
    (ra.is_finite() && dec.is_finite()).then_some((ra, dec))
}

/// A partition's rows, as one file or as a directory holding several.
#[derive(Debug)]
pub enum Partitioned {
    One(RemoteFile),
    Many(RemoteDir),
}

/// Which columns a query against this catalog reads a position out of, and which column
/// accelerates it — the catalog's answer, which a request may override.
#[derive(Debug, Clone, Copy)]
pub struct Columns<'a> {
    pub ra: &'a str,
    pub dec: &'a str,
    pub healpix: (&'a str, u8),
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::time::Duration;

    use datafusion::arrow::array::{Float64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::metadata::{
        ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter,
    };
    use futures::executor::block_on;
    use url::Url;

    use super::*;
    use crate::access::AccessPolicy;
    use crate::config::{AccessConfig, LimitsConfig};
    use crate::hats::cache::{Catalogs, Lifetime};
    use crate::storage::StorageOptions;
    use crate::storage::materialize::Transfers;

    /// The cells `small_sky_order3_source` is cut into, cut down to a handful. Mixed with
    /// a coarse one, which is what a real catalog does where the sky is empty.
    const CELLS: [(u8, u64); 4] = [(3, 264), (3, 707), (1, 43), (3, 708)];

    /// The prefix the fixture is mounted under, which is also how a request names it: a
    /// local directory is addressed in the mounts' url space rather than on the disk.
    const MOUNT: &str = "/catalog";

    fn dir_of(root: &Path) -> RemoteDir {
        let mounts = crate::access::mount::Mounts::new(
            &[crate::config::MountConfig {
                path: MOUNT.to_owned(),
                source: root.display().to_string(),
                serve: false,
                follow_symlinks: false,
                catalog_cache_seconds: None,
                storage: StorageOptions::default(),
                filenames: None,
            }],
            &crate::config::DataConfig::default(),
        )
        .unwrap();
        let policy = AccessPolicy::new(&AccessConfig::default(), Arc::new(mounts), None).unwrap();
        let url = Url::parse(&format!("file://{MOUNT}/")).unwrap();
        crate::storage::open_dir(
            &url,
            &StorageOptions::default(),
            &policy,
            &Arc::new(Transfers::new(&LimitsConfig::default())),
        )
        .unwrap()
    }

    fn opened_in(
        root: &Path,
        cache: &CatalogCache,
        max_metadata_bytes: u64,
    ) -> Result<HatsCatalog, ApiError> {
        block_on(HatsCatalog::open(dir_of(root), cache, max_metadata_bytes))
    }

    fn opened(root: &Path, max_metadata_bytes: u64) -> Result<HatsCatalog, ApiError> {
        opened_in(root, &CatalogCache::off(), max_metadata_bytes)
    }

    fn open(root: &Path) -> HatsCatalog {
        opened(root, u64::MAX).unwrap()
    }

    fn partitions_of(catalog: &HatsCatalog) -> Arc<HatsPartitionList> {
        block_on(catalog.partitions()).unwrap()
    }

    /// A catalog with `properties` and a `dataset/` tree, and whichever of the two
    /// partition sources the case wants.
    fn catalog(properties: &str, partition_info: bool, metadata: bool) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join("hats.properties"), properties).unwrap();
        if partition_info {
            write_partition_info(root, &CELLS);
        }
        let mut written = Vec::new();
        for (order, pixel) in CELLS {
            let cell = HatsPartition::new(order, pixel);
            let path = cell.path(".parquet");
            let file = root.join(&path);
            fs::create_dir_all(file.parent().unwrap()).unwrap();
            // A row per pixel number, so the counts differ between partitions and a total
            // that came out of one file is visible.
            written.push((
                path.trim_start_matches("dataset/").to_owned(),
                write_partition(&file, usize::try_from(pixel % 7 + 1).unwrap()),
            ));
        }
        if metadata {
            write_metadata(&root.join(partitions::METADATA), &written);
        }
        dir
    }

    fn write_partition_info(root: &Path, cells: &[(u8, u64)]) {
        let mut csv = String::from("Norder,Npix\n");
        for (order, pixel) in cells {
            csv.push_str(&format!("{order},{pixel}\n"));
        }
        fs::write(root.join(partitions::PARTITION_INFO), csv).unwrap();
    }

    fn write_partition(path: &Path, rows: usize) -> ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ra", DataType::Float64, false),
            Field::new("dec", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![1.0; rows])),
                Arc::new(Float64Array::from(vec![2.0; rows])),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        ParquetMetaDataReader::new()
            .parse_and_finish(&bytes::Bytes::from(fs::read(path).unwrap()))
            .unwrap()
    }

    /// `_metadata` the way an importer writes it: every partition's row groups in one
    /// footer, each tagged with the path of the file it came from.
    fn write_metadata(path: &Path, partitions: &[(String, ParquetMetaData)]) {
        let mut first = None;
        let mut groups = Vec::new();
        for (name, metadata) in partitions {
            first.get_or_insert_with(|| metadata.file_metadata().clone());
            for group in metadata.row_groups() {
                let columns = group
                    .columns()
                    .iter()
                    .map(|chunk| {
                        chunk
                            .clone()
                            .into_builder()
                            .set_file_path(name.clone())
                            .build()
                            .unwrap()
                    })
                    .collect();
                groups.push(
                    group
                        .clone()
                        .into_builder()
                        .set_column_metadata(columns)
                        .build()
                        .unwrap(),
                );
            }
        }
        let combined = ParquetMetaData::new(first.unwrap(), groups);
        let file = fs::File::create(path).unwrap();
        ParquetMetaDataWriter::new(file, &combined)
            .finish()
            .unwrap();
    }

    const PROPERTIES: &str = "\
obs_collection=small
hats_col_ra=ra
hats_col_dec=dec
hats_order=3
";

    /// All three sources describe the same catalog, so all three must produce the same
    /// partitions. Only what they know beside the cells differs.
    #[test]
    fn every_source_finds_the_same_partitions() {
        let mut spellings = Vec::new();
        for (partition_info, metadata, expected) in [
            (true, true, partitions::Source::PartitionInfo),
            (false, true, partitions::Source::Metadata),
            (false, false, partitions::Source::Listing),
        ] {
            let dir = catalog(PROPERTIES, partition_info, metadata);
            let partitions = partitions_of(&open(dir.path()));
            assert_eq!(partitions.source(), expected);
            spellings.push(
                partitions
                    .cells()
                    .iter()
                    .map(|cell| (cell.order, cell.pixel))
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(spellings[0], spellings[1]);
        assert_eq!(spellings[1], spellings[2]);
        assert_eq!(spellings[0].len(), CELLS.len());
        // And in HEALPix order, which is neither the order they were written in nor the
        // order their names sort in: order-1 pixel 43 covers order-3 pixels 688 to 703, so
        // it lands between 264 and 707 rather than after all of them.
        assert_eq!(spellings[0], vec![(3, 264), (1, 43), (3, 707), (3, 708)]);
    }

    /// The sizes are what `_metadata` is worth over the cheaper source, and the reason it
    /// is worth reading even when `partition_info.csv` answered.
    #[test]
    fn only_the_footer_source_knows_how_big_a_partition_is() {
        let dir = catalog(PROPERTIES, false, true);
        for cell in partitions_of(&open(dir.path())).cells() {
            assert_eq!(cell.rows, Some(cell.pixel % 7 + 1), "{cell:?}");
            assert!(cell.bytes.is_some_and(|bytes| bytes > 0), "{cell:?}");
        }

        let dir = catalog(PROPERTIES, true, true);
        for cell in partitions_of(&open(dir.path())).cells() {
            assert_eq!(cell.rows, None);
        }
    }

    /// The catalog says which columns are a position, which is what a lone parquet file
    /// cannot, and names its own accelerator or takes the recommendation.
    #[test]
    fn a_catalog_supplies_the_column_names_a_request_would_have_had_to() {
        let dir = catalog(PROPERTIES, true, false);
        let unsaid = open(dir.path());
        let columns = unsaid.columns().unwrap();
        assert_eq!((columns.ra, columns.dec), ("ra", "dec"));
        assert_eq!(columns.healpix, ("_healpix_29", 29));

        let dir = catalog(
            "hats_col_ra=objra\nhats_col_dec=objdec\nhats_col_healpix=hp\n\
             hats_col_healpix_order=13\n",
            true,
            false,
        );
        let named = open(dir.path());
        let columns = named.columns().unwrap();
        assert_eq!((columns.ra, columns.dec), ("objra", "objdec"));
        assert_eq!(columns.healpix, ("hp", 13));
    }

    /// The order that decides what a covering is built against is the one the files are
    /// at, not the one the properties file claims.
    #[test]
    fn the_partitions_decide_the_catalogs_order() {
        let dir = catalog("hats_order=9\n", true, false);
        assert_eq!(partitions_of(&open(dir.path())).order(), Some(3));
    }

    /// A directory that is not a catalog is a 404 and not a 500: the caller may simply
    /// have named the directory above the one they meant.
    #[test]
    fn a_directory_with_no_properties_is_not_a_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let error = opened(dir.path(), u64::MAX).unwrap_err();
        assert_eq!(error.status(), http::StatusCode::NOT_FOUND, "{error}");
    }

    /// `hats.properties` is the spelling to prefer, and the deprecated one still opens a
    /// catalog that carries only it. A catalog carrying both is described by the preferred
    /// one, which is what makes the order an order rather than a coincidence.
    #[test]
    fn the_deprecated_name_for_the_properties_file_is_the_fallback() {
        let dir = catalog(PROPERTIES, true, false);
        fs::rename(
            dir.path().join("hats.properties"),
            dir.path().join("properties"),
        )
        .unwrap();
        assert_eq!(open(dir.path()).properties().name(), Some("small"));

        fs::write(
            dir.path().join("hats.properties"),
            PROPERTIES.replace("obs_collection=small", "obs_collection=preferred"),
        )
        .unwrap();
        assert_eq!(open(dir.path()).properties().name(), Some("preferred"));
    }

    /// Over the cap the footer is not fetched at all, and the listing answers instead —
    /// the same partitions, without the sizes.
    #[test]
    fn a_footer_too_large_to_fetch_falls_through_to_the_listing() {
        let dir = catalog(PROPERTIES, false, true);
        let listed = partitions_of(&opened(dir.path(), 1).unwrap());
        assert_eq!(listed.source(), partitions::Source::Listing);
        assert_eq!(listed.len(), CELLS.len());
    }

    /// Derived from the cell and the catalog's own suffix, and it has to name a file that
    /// is really there — the `Dir=` grouping is the part a reader gets wrong.
    #[test]
    fn a_partition_url_names_the_file_that_holds_it() {
        let dir = catalog(PROPERTIES, true, false);
        let opened = open(dir.path());
        let data = DataFiles::default();
        for cell in partitions_of(&opened).cells() {
            let files = block_on(opened.files(cell, &data)).unwrap();
            let [file] = files.as_slice() else {
                panic!("{cell:?} came back as {} files", files.len());
            };
            assert!(
                file.url.to_file_path().unwrap().is_file(),
                "{}",
                file.url.as_str()
            );
        }
    }

    /// Rewrite a fixture the way a directory-partitioned catalog is written: `Npix=p/`
    /// holding two parts and a marker beside them.
    fn as_directories(root: &Path) {
        for (order, pixel) in CELLS {
            let one = root.join(HatsPartition::new(order, pixel).path(".parquet"));
            let many = root.join(HatsPartition::new(order, pixel).path("/"));
            fs::create_dir_all(&many).unwrap();
            fs::rename(&one, many.join("part0.parquet")).unwrap();
            fs::copy(many.join("part0.parquet"), many.join("part1.parquet")).unwrap();
            fs::write(many.join("_SUCCESS"), b"").unwrap();
        }
    }

    /// `hats_npix_suffix=/` makes a partition a directory of files, which is what a catalog
    /// large enough to split one writes — ZTF DR24's objects among them. Every file in it
    /// is read, and what in it counts as data is the configured list's answer.
    #[test]
    fn a_partition_may_be_a_directory_of_files() {
        let dir = catalog(&format!("{PROPERTIES}hats_npix_suffix=/\n"), true, false);
        as_directories(dir.path());

        let opened = open(dir.path());
        let data = DataFiles::default();
        for cell in partitions_of(&opened).cells() {
            let files = block_on(opened.files(cell, &data)).unwrap();
            let mut names: Vec<_> = files
                .iter()
                .map(|file| {
                    file.url
                        .path_segments()
                        .unwrap()
                        .next_back()
                        .unwrap()
                        .to_owned()
                })
                .collect();
            names.sort();
            // Both parts, and not the marker beside them.
            assert_eq!(names, vec!["part0.parquet", "part1.parquet"], "{cell:?}");
        }
    }

    /// Listing each partition and walking the whole dataset are two costs for one answer, so
    /// whichever is taken — and a walk that answered for partitions nobody asked about yet —
    /// gives the same names as each partition listed on its own.
    #[test]
    fn a_walk_and_a_listing_per_partition_name_the_same_files() {
        let dir = catalog(&format!("{PROPERTIES}hats_npix_suffix=/\n"), true, false);
        as_directories(dir.path());
        let data = DataFiles::default();

        let each = open(dir.path());
        let cells = partitions_of(&each).cells().to_vec();
        let listed = block_on(each.names(&cells[..1], &data)).unwrap();

        // Every partition at once is more than one page's worth of requests would be, so this
        // walks, and fills every partition's names from that one listing.
        let walked = open(dir.path());
        let all = block_on(walked.names(&cells, &data)).unwrap();
        assert_eq!(all[0], listed[0]);
        for names in &all {
            let mut names = names.clone();
            names.sort();
            assert_eq!(names, vec!["part0.parquet", "part1.parquet"]);
        }
    }

    /// A collection is followed one hop, downwards, to the catalog it calls its primary
    /// table.
    #[test]
    fn a_collection_opens_as_the_catalog_inside_it() {
        let dir = catalog(PROPERTIES, true, false);
        let root = tempfile::tempdir().unwrap();
        let inside = root.path().join("the_catalog");
        fs::rename(dir.path(), &inside).unwrap();
        fs::write(
            root.path().join(properties::COLLECTION),
            "obs_collection=a_collection\nhats_primary_table_url=the_catalog\n",
        )
        .unwrap();

        let opened = open(root.path());
        assert_eq!(opened.properties().name(), Some("small"));
        assert_eq!(
            opened.collection().and_then(Properties::name),
            Some("a_collection")
        );
        assert_eq!(opened.within(), "the_catalog/");
        assert_eq!(partitions_of(&opened).len(), CELLS.len());
    }

    /// A collection's primary table is a file this service reads, so following one that
    /// points outside the collection would let that file choose where the service goes
    /// next. Every such spelling is refused, and the caller is told to name the catalog.
    #[test]
    fn a_collection_is_not_followed_out_of_itself() {
        for named in [
            "s3://bucket/elsewhere",
            "https://example.com/catalog",
            "/etc",
            "../sibling",
            "the_catalog/../../escape",
        ] {
            let root = tempfile::tempdir().unwrap();
            fs::write(
                root.path().join(properties::COLLECTION),
                format!("hats_primary_table_url={named}\n"),
            )
            .unwrap();
            let error = opened(root.path(), u64::MAX).unwrap_err();
            assert_eq!(error.status(), http::StatusCode::BAD_REQUEST, "{named}");
            assert!(
                error.to_string().contains("hats_primary_table_url"),
                "{named}: {error}"
            );
        }
    }

    /// The schema is `_common_metadata`'s where the catalog has one and the first
    /// partition's where it has not, and the two say the same thing about a catalog whose
    /// partitions all share one.
    #[tokio::test]
    async fn the_schema_comes_from_common_metadata_else_a_partition() {
        let dir = catalog(PROPERTIES, true, false);
        let data = DataFiles::default();
        let catalog = HatsCatalog::open(dir_of(dir.path()), &CatalogCache::off(), u64::MAX)
            .await
            .unwrap();
        assert!(catalog.common_schema().await.is_none());
        let from_partition = catalog.schema(&data).await.unwrap();

        fs::copy(
            dir.path().join(HatsPartition::new(3, 264).path(".parquet")),
            dir.path().join(COMMON_METADATA),
        )
        .unwrap();
        let catalog = HatsCatalog::open(dir_of(dir.path()), &CatalogCache::off(), u64::MAX)
            .await
            .unwrap();
        assert!(catalog.common_schema().await.is_some());
        assert_eq!(catalog.schema(&data).await.unwrap(), from_partition);
    }

    /// Everything a catalog is read for is kept across requests while its lifetime lasts: a
    /// catalog changed on disk underneath is answered as it was, properties and parts alike.
    #[tokio::test]
    async fn a_catalog_is_answered_from_the_cache_while_its_lifetime_lasts() {
        let dir = catalog(PROPERTIES, true, false);
        let cache = Catalogs::new(1 << 20).with_lifetime(Lifetime::Forever);
        let data = DataFiles::default();
        let first = HatsCatalog::open(dir_of(dir.path()), &cache, u64::MAX)
            .await
            .unwrap();
        let partitions = first.partitions().await.unwrap();
        let position = first.example_position(&data).await;
        assert_eq!(position, Some((1.0, 2.0)));

        // Republished: another name, one partition, and no rows anywhere to take a position
        // from. None of it is read again.
        fs::write(
            dir.path().join("hats.properties"),
            PROPERTIES.replace("obs_collection=small", "obs_collection=republished"),
        )
        .unwrap();
        write_partition_info(dir.path(), &CELLS[..1]);
        fs::remove_dir_all(dir.path().join(DATASET_DIR)).unwrap();

        let second = HatsCatalog::open(dir_of(dir.path()), &cache, u64::MAX)
            .await
            .unwrap();
        assert_eq!(second.properties().name(), Some("small"));
        assert_eq!(second.partitions().await.unwrap().len(), partitions.len());
        assert_eq!(second.example_position(&data).await, position);

        // And the same catalog with nothing kept is read as it is now.
        let now = HatsCatalog::open(dir_of(dir.path()), &CatalogCache::off(), u64::MAX)
            .await
            .unwrap();
        assert_eq!(now.properties().name(), Some("republished"));
        assert_eq!(now.partitions().await.unwrap().len(), 1);
    }

    /// Once the lifetime is over the properties are read again, and with them every part:
    /// a partition list from before a republication is never paired with properties from
    /// after it.
    #[tokio::test]
    async fn a_part_goes_when_its_catalogs_lifetime_does() {
        let dir = catalog(PROPERTIES, true, false);
        let cache = Catalogs::new(1 << 20).with_lifetime(Lifetime::For(Duration::from_millis(300)));
        let first = HatsCatalog::open(dir_of(dir.path()), &cache, u64::MAX)
            .await
            .unwrap();
        assert_eq!(first.partitions().await.unwrap().len(), CELLS.len());

        fs::write(
            dir.path().join("hats.properties"),
            PROPERTIES.replace("obs_collection=small", "obs_collection=republished"),
        )
        .unwrap();
        write_partition_info(dir.path(), &CELLS[..1]);
        let held = HatsCatalog::open(dir_of(dir.path()), &cache, u64::MAX)
            .await
            .unwrap();
        assert_eq!(held.properties().name(), Some("small"));
        assert_eq!(held.partitions().await.unwrap().len(), CELLS.len());

        tokio::time::sleep(Duration::from_millis(400)).await;
        let reread = HatsCatalog::open(dir_of(dir.path()), &cache, u64::MAX)
            .await
            .unwrap();
        assert_eq!(reread.properties().name(), Some("republished"));
        assert_eq!(reread.partitions().await.unwrap().len(), 1);
    }

    /// A read that fails is not kept: the next request reads again and gets the catalog.
    #[tokio::test]
    async fn a_failed_read_is_not_kept() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Catalogs::new(1 << 20).with_lifetime(Lifetime::Forever);
        let error = HatsCatalog::open(dir_of(dir.path()), &cache, u64::MAX)
            .await
            .unwrap_err();
        assert_eq!(error.status(), http::StatusCode::NOT_FOUND);

        let written = catalog(PROPERTIES, true, false);
        for entry in fs::read_dir(written.path()).unwrap() {
            let entry = entry.unwrap();
            fs::rename(entry.path(), dir.path().join(entry.file_name())).unwrap();
        }
        let opened = HatsCatalog::open(dir_of(dir.path()), &cache, u64::MAX)
            .await
            .unwrap();
        assert_eq!(opened.properties().name(), Some("small"));
    }
}
