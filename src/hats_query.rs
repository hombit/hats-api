//! A request against a whole HATS catalog rather than against one file of it.
//!
//! Three modules meet here and none of them could do this alone. `hats/` reads a catalog's
//! own files and decides nothing about a request; `healpix.rs` answers questions about cells
//! and knows nothing about a catalog's contents; `query.rs` runs a selection against one
//! file and knows nothing about either. This opens the catalog, settles which columns hold a
//! position, chooses the partitions the region reaches, and reads them.
//!
//! **Partitions are read in the catalog's own order, which is HEALPix order.** A cell's
//! number is where it is on the sky, so that order puts neighbouring rows near each other
//! and makes a `limit` a coherent piece of sky rather than an arbitrary sample. It costs
//! nothing — the partitions have to be enumerated anyway — which is why this promises more
//! about order than a request naming one url does. What it does not promise is the order
//! *within* a partition, which is [`Order`]'s business and unchanged.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use futures::StreamExt;
use futures::stream;

use crate::config::LimitsConfig;
use crate::data::DataFiles;
use crate::error::ApiError;
use crate::hats::{Catalog, HatsPartition, Partitioned};
use crate::healpix::{Cover, Coverage, Detail};
use crate::query::{self, Order, Predicate, Projection, QueryResult, Selection};
use crate::region::{self, Absence, Healpix, Region, Spatial};
use crate::sql;
use crate::storage::{RemoteDir, RemoteFile};

/// What one request against a catalog may spend before it is refused.
#[derive(Debug, Clone, Copy)]
pub struct CatalogLimits {
    /// How much of `_metadata` may be fetched to learn the partition list.
    pub max_metadata_bytes: u64,
    /// How many partitions one request may read, how many bytes it may fetch, and how many
    /// rows it may return. Whichever is reached first stops it.
    ///
    /// Only the first can be checked before anything is read — the partition list says how
    /// many there are, and the other two are counters that have to accumulate. So the
    /// partition count is what actually protects the origin, and the other two are what
    /// catch a request whose few partitions turn out to be enormous.
    pub max_partitions: usize,
    pub max_bytes_fetched: u64,
    pub max_rows: usize,
    /// How many partitions are read at once. Not a bound — a performance setting, and the
    /// reason the two accumulating bounds overshoot rather than stop dead.
    pub max_concurrent_partitions: usize,
}

/// Which bound stopped a request, and the two numbers it turned on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exceeded {
    Partitions { reached: usize, allowed: usize },
    Bytes { reached: u64, allowed: u64 },
    Rows { allowed: usize },
}

impl fmt::Display for Exceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Partitions { reached, allowed } => write!(
                f,
                "this request reaches {reached} partitions; this server reads at most \
                 {allowed} in one request"
            ),
            Self::Bytes { reached, allowed } => write!(
                f,
                "this request fetched {reached} bytes; this server fetches at most \
                 {allowed} in one request"
            ),
            Self::Rows { allowed } => write!(
                f,
                "this request matches more than {allowed} rows; this server returns at \
                 most that in one request"
            ),
        }
    }
}

/// What running a request came to.
///
/// A bound reached is not an error here: the caller gets the work list instead, and
/// building it needs the [`Search`] that ran. So it comes back as a value rather than
/// through `?`, and the route decides what to answer with.
#[derive(Debug)]
pub enum Outcome {
    /// Boxed because the other variant is three integers and this one carries every row of
    /// the answer, and an enum is as large as its largest variant wherever it is passed.
    Rows(Box<CatalogResult>),
    /// Nothing is returned with this. Half an answer that a caller cannot tell from a whole
    /// one is the failure this service keeps finding, and a truncated set of rows under no
    /// promised order is exactly that.
    TooMuchWork(Exceeded),
}

impl From<&LimitsConfig> for CatalogLimits {
    fn from(config: &LimitsConfig) -> Self {
        Self {
            max_metadata_bytes: config.max_catalog_metadata_bytes.as_u64(),
            max_partitions: config.max_partitions,
            max_bytes_fetched: config.max_bytes_fetched.as_u64(),
            max_rows: config.max_rows,
            max_concurrent_partitions: config.max_concurrent_partitions,
        }
    }
}

/// The columns a request reads a position out of, as the catalog names them.
///
/// **The catalog is the only source.** A request against a catalog does not get to name its
/// own: `hats_col_ra`, `hats_col_dec` and `hats_col_healpix` are what the catalog says its
/// columns are, and a caller overriding them would be describing a file they can see less of
/// than the catalog does. The routes refuse those fields rather than honour them, and a
/// caller who really wants to test a different pair of columns names the file itself.
#[derive(Debug, Clone)]
pub struct Columns {
    pub ra: String,
    pub dec: String,
    /// The index column and the order its values are at.
    ///
    /// Always present, because a catalog always offers a candidate — `_healpix_29` at 29
    /// where nothing names one.
    pub healpix: (String, u8),
    /// Which of those two it is, and therefore what a file without the column means.
    ///
    /// [`Absence::Ignore`] only where nobody named a column. That is the whole of what makes
    /// the default safe to try: a catalog whose files have no `_healpix_29` is queried on the
    /// geometry alone, rather than refused for not having a column it never claimed.
    pub absence: Absence,
}

/// One partition this request will read, and what its rows still need.
#[derive(Debug, Clone)]
pub struct Chosen {
    pub partition: HatsPartition,
    /// Never [`Cover::Outside`] — those are the partitions this request does not read.
    pub cover: Cover,
}

/// What to read from each partition, in whichever vocabulary the request used.
///
/// The spatial part is deliberately not here. It differs per partition — a partition the
/// region contains whole needs none at all — and deciding that is this module's job rather
/// than the request's.
#[derive(Debug, Default)]
pub struct CatalogSelection<'a> {
    pub projection: Projection<'a>,
    pub predicate: Predicate<'a>,
    /// The shapes, as a union. `None` is the whole catalog, which is bounded only by
    /// [`CatalogLimits::max_partitions`].
    pub regions: Option<&'a [Region]>,
    pub limit: Option<usize>,
}

/// A request resolved against a catalog, with nothing read from any partition yet.
///
/// Both HATS routes stop here: one renders it as a work list, the other runs it. They must
/// choose the same partitions, so they ask for them the same way.
#[derive(Debug)]
pub struct Search {
    catalog: Catalog,
    /// Resolved only where the request carries a region, since that is the only thing that
    /// reads a position out of a row.
    columns: Option<Columns>,
    chosen: Vec<Chosen>,
}

impl Search {
    /// Open the catalog and choose the partitions, reading no data.
    pub async fn resolve(
        dir: RemoteDir,
        regions: Option<&[Region]>,
        limits: CatalogLimits,
    ) -> Result<Self, ApiError> {
        let catalog = Catalog::open(dir, limits.max_metadata_bytes).await?;
        // Only where there is a region to test. A catalog that does not name its position
        // columns is perfectly readable by a request that asks no spatial question, and
        // refusing one for want of a column it would never have used is refusing a request
        // that is not wrong.
        let columns = match regions {
            None => None,
            Some(_) => Some(columns(&catalog)?),
        };
        let chosen = match regions {
            // No region is every partition, and none of them needs a spatial test.
            None => catalog
                .partitions()
                .cells()
                .iter()
                .map(|partition| Chosen {
                    partition: partition.clone(),
                    cover: Cover::Inside,
                })
                .collect(),
            Some(regions) => {
                let shapes = region::shapes(regions)?;
                // The catalog's own deepest order, which is what the covering's depth is
                // taken relative to. A catalog with no partitions at all has no order and
                // no partitions to choose, so the covering it gets is beside the point.
                let catalog_order = catalog.order().unwrap_or_default();
                let coverage = Coverage::of(&shapes, Detail::Partitions { catalog_order });
                coverage
                    .reaches(catalog.partitions())
                    .into_iter()
                    .map(|reached| Chosen {
                        partition: reached.partition.clone(),
                        cover: reached.cover,
                    })
                    .collect()
            }
        };
        Ok(Self {
            catalog,
            columns,
            chosen,
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn columns(&self) -> Option<&Columns> {
        self.columns.as_ref()
    }

    /// The partitions, in the catalog's own order.
    pub fn chosen(&self) -> &[Chosen] {
        &self.chosen
    }

    /// Read the chosen partitions and gather their rows.
    ///
    /// **Several at a time, and the answer is still in the catalog's order.** `buffered`
    /// keeps `max_concurrent_partitions` reads in flight and yields them by position, so the
    /// parallelism costs nothing in order — which matters, because the order is what makes a
    /// `limit` here a coherent piece of sky rather than whichever partition finished first.
    ///
    /// **The two accumulating bounds are checked between partitions, not within them.** A
    /// counter shared across concurrent scans, read often enough to stop one mid-file, would
    /// serialize the thing it is bounding. So a request overshoots by whatever the partitions
    /// already in flight go on to fetch, and the partition count is what keeps that bounded:
    /// it is checked before anything is read, so nothing else has to be exact.
    pub async fn run(
        &self,
        selection: &CatalogSelection<'_>,
        data: &DataFiles,
        limits: sql::Limits,
        bounds: CatalogLimits,
    ) -> Result<Outcome, ApiError> {
        if self.chosen.len() > bounds.max_partitions {
            return Ok(Outcome::TooMuchWork(Exceeded::Partitions {
                reached: self.chosen.len(),
                allowed: bounds.max_partitions,
            }));
        }
        // Built before the stream rather than inside a closure it calls: a closure returning
        // a future that borrows its argument has to satisfy a higher-ranked bound the
        // compiler cannot infer here, and materializing the futures sidesteps it. Nothing is
        // read until the stream is polled, so the eager `collect` costs nothing.
        let reads = self
            .chosen
            .iter()
            .map(|chosen| self.read(chosen, selection, data, limits))
            .collect::<Vec<_>>();
        let mut reads = stream::iter(reads).buffered(bounds.max_concurrent_partitions.max(1));

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut schema: Option<SchemaRef> = None;
        let mut data_bytes_read = 0;
        let mut rows = 0;
        let mut partitions_read = 0;
        let mut source = None;
        while let Some(read) = reads.next().await {
            let read = read?;
            partitions_read += 1;
            data_bytes_read += read.data_bytes_read;
            rows += read.rows;
            if let Some(read_schema) = read.schema {
                schema.get_or_insert(read_schema);
            }
            if source.is_none() {
                source = read.source;
            }
            batches.extend(read.batches);
            if data_bytes_read > bounds.max_bytes_fetched {
                return Ok(Outcome::TooMuchWork(Exceeded::Bytes {
                    reached: data_bytes_read,
                    allowed: bounds.max_bytes_fetched,
                }));
            }
            if rows > bounds.max_rows {
                return Ok(Outcome::TooMuchWork(Exceeded::Rows {
                    allowed: bounds.max_rows,
                }));
            }
        }
        // Each partition was read with the whole `limit` as its own, so the total can exceed
        // it. Trimming here rather than there is what a concurrent read costs: a partition
        // cannot know how many rows the ones before it matched. The order is the catalog's,
        // so which rows survive the trim is the same every time.
        if let Some(limit) = selection.limit {
            truncate(&mut batches, limit);
        }

        Ok(Outcome::Rows(Box::new(CatalogResult {
            rows: QueryResult {
                schema: schema.unwrap_or_else(|| Arc::new(Schema::empty())),
                batches,
                data_bytes_read,
            },
            partitions_read,
            source,
        })))
    }

    /// One partition: every file of it, with the region cut down to that partition's cell.
    async fn read(
        &self,
        chosen: &Chosen,
        selection: &CatalogSelection<'_>,
        data: &DataFiles,
        limits: sql::Limits,
    ) -> Result<Read, ApiError> {
        let mut read = Read::default();
        for file in self
            .catalog
            .partition(&chosen.partition)?
            .files(data)
            .await?
        {
            let per_file = Selection {
                projection: selection.projection,
                predicate: selection.predicate,
                spatial: self.spatial(selection.regions, chosen),
                // The caller's whole limit, not a share of it: this read does not know what
                // the others matched. It caps what one partition returns, and the total is
                // trimmed once every partition is in.
                limit: selection.limit,
            };
            let result = query::run(&file, &per_file, limits, Order::Unspecified).await?;
            read.data_bytes_read += result.data_bytes_read;
            read.rows += result.num_rows();
            read.schema
                .get_or_insert_with(|| Arc::clone(&result.schema));
            if read.source.is_none() {
                read.source = Some(file);
            }
            read.batches.extend(result.batches);
        }
        Ok(read)
    }

    /// The same work, as one entry per file, without reading a row of any of them.
    ///
    /// **Every entry names a path below the catalog and never a url.** The urls this holds
    /// are the store's, and for a mounted catalog a store's url is the operator's absolute
    /// path on disk — the one thing that may not reach a caller. A path joined onto the url
    /// the caller themselves wrote is the same object said in the spelling they can use, so
    /// the caller's url is the one the entry is built from, and this hands back the half
    /// that is the catalog's own.
    ///
    /// A partition written as a directory needs a listing to enumerate. That is metadata
    /// rather than rows, so it is within what this route promises, but it is one request per
    /// such partition and the ordinary catalog pays none.
    pub async fn entries(&self, data: &DataFiles) -> Result<Vec<Entry>, ApiError> {
        let suffix = self.catalog.properties().npix_suffix();
        let mut entries = Vec::new();
        for chosen in &self.chosen {
            let partition = &chosen.partition;
            let path = partition.path(suffix);
            match self.catalog.partition(partition)? {
                Partitioned::One(_) => entries.push(Entry {
                    order: partition.order,
                    pixel: partition.pixel,
                    path,
                    cover: chosen.cover,
                    // Only `_metadata` knows it, so only a catalog whose partition list came
                    // from there has an estimate to give.
                    estimated_bytes: partition.bytes,
                }),
                Partitioned::Many(_) => {
                    for file in self.catalog.partition(partition)?.files(data).await? {
                        let Some(name) = file.url.path_segments().and_then(|mut s| s.next_back())
                        else {
                            continue;
                        };
                        entries.push(Entry {
                            order: partition.order,
                            pixel: partition.pixel,
                            // The name inside the partition, which is the catalog's own and
                            // says nothing about where the catalog is.
                            path: format!("{path}{name}"),
                            cover: chosen.cover,
                            // The partition's bytes are the whole directory's, so they are
                            // not this file's and are not divisible into one.
                            estimated_bytes: None,
                        });
                    }
                }
            }
        }
        Ok(entries)
    }

    /// The spatial constraint one partition's rows still have to meet.
    ///
    /// `None` twice over, and the two mean different things. A request with no region asks
    /// no spatial question of any partition. A partition the region *contains* has already
    /// answered it: every row in it qualifies, so a test there could only cost time — which
    /// is the whole reason the covering is computed from both sides.
    fn spatial<'a>(
        &'a self,
        regions: Option<&'a [Region]>,
        chosen: &Chosen,
    ) -> Option<Spatial<'a>> {
        let regions = regions?;
        let columns = self.columns.as_ref()?;
        if chosen.cover == Cover::Inside {
            return None;
        }
        Some(Spatial {
            regions,
            ra_column: Some(&columns.ra),
            dec_column: Some(&columns.dec),
            healpix: Some(Healpix {
                column: &columns.healpix.0,
                order: columns.healpix.1,
                absence: columns.absence,
            }),
            partition: Some((chosen.partition.order, chosen.partition.pixel)),
        })
    }
}

/// What one partition came to, before it is folded in with the rest.
#[derive(Debug, Default)]
struct Read {
    schema: Option<SchemaRef>,
    batches: Vec<RecordBatch>,
    data_bytes_read: u64,
    rows: usize,
    source: Option<RemoteFile>,
}

/// Drop everything past `limit` rows, keeping the batches in the order they arrived.
///
/// The batch that straddles the limit is sliced rather than dropped, which is a view onto
/// the same buffers and copies nothing.
fn truncate(batches: &mut Vec<RecordBatch>, limit: usize) {
    let mut kept = 0;
    for index in 0..batches.len() {
        let Some(batch) = batches.get_mut(index) else {
            break;
        };
        if kept >= limit {
            batches.truncate(index);
            return;
        }
        let room = limit - kept;
        if batch.num_rows() > room {
            *batch = batch.slice(0, room);
        }
        kept += batch.num_rows();
    }
}

/// One file of the answer, named by where it sits below the catalog.
#[derive(Debug, Clone)]
pub struct Entry {
    pub order: u8,
    pub pixel: u64,
    /// Below the catalog directory — `dataset/Norder=3/Dir=0/Npix=707.parquet`. A path and
    /// not a url, so that whoever renders it joins it onto the url the caller wrote.
    pub path: String,
    /// Whether this file's rows still need the region tested against them.
    pub cover: Cover,
    /// Bytes on the wire to read **the whole file**, where the catalog said — which only
    /// `_metadata` does, so it is absent for a catalog found any other way.
    ///
    /// **Not what the query will fetch.** A projection with the predicate pruned reads a
    /// percent or two of a wide partition, so this is larger than the answer by one or two
    /// orders of magnitude. What it does bound is how big one of these requests could get,
    /// which is what a client deciding fan-out concurrency wants. Reading `_metadata` to
    /// fill it in where the catalog did not is not worth a large `GET` for a number nobody
    /// can use as a cost.
    pub estimated_bytes: Option<u64>,
}

/// The rows, and what reading them cost.
#[derive(Debug)]
pub struct CatalogResult {
    pub rows: QueryResult,
    /// How many partitions were opened, which under a `limit` is fewer than were chosen.
    pub partitions_read: usize,
    /// One of the files read, for the parquet writer to copy a layout from. Absent where
    /// nothing was read at all.
    pub source: Option<RemoteFile>,
}

/// What the catalog says its own columns are.
fn columns(catalog: &Catalog) -> Result<Columns, ApiError> {
    let properties = catalog.properties();
    let (ra, dec) = properties.coordinate_columns().ok_or_else(|| {
        ApiError::bad_request(
            "this catalog does not name its position columns, so a region cannot be \
             tested against it; query one of its files directly",
        )
    })?;
    let (column, order) = properties.healpix_column()?;
    Ok(Columns {
        ra: ra.to_owned(),
        dec: dec.to_owned(),
        healpix: (column.to_owned(), order),
        // The catalog's own `hats_col_healpix` is a claim about its files; the `_healpix_29`
        // this falls back to is HATS's recommendation, which a catalog is free not to have
        // taken.
        absence: match properties.names_healpix_column() {
            true => Absence::Refuse,
            false => Absence::Ignore,
        },
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fs;
    use std::path::Path;

    use datafusion::arrow::array::{Array, Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::parquet::arrow::ArrowWriter;
    use url::Url;

    use super::*;
    use crate::access::AccessPolicy;
    use crate::config::{AccessConfig, DataConfig, LimitsConfig, MountConfig};
    use crate::materialize::Transfers;
    use crate::mount::Mounts;
    use crate::region::Region;
    use crate::storage::StorageOptions;

    /// The order the fixture is partitioned at.
    const ORDER: u8 = 3;

    /// Its four partitions: the children of one order-2 cell, so they are contiguous on the
    /// sky and a small cone lands inside one of them.
    const CELLS: [u64; 4] = [64, 65, 66, 67];

    /// How much finer than a partition the rows are spread: every order-6 child's centre,
    /// which is 64 points inside each partition and every one of them certainly inside it.
    const ROW_ORDER: u8 = 6;

    const MOUNT: &str = "/catalog";

    /// One row of the fixture, before it is written.
    #[derive(Debug, Clone, Copy)]
    struct Point {
        id: i64,
        ra: f64,
        dec: f64,
    }

    /// Every row of the catalog, partition by partition, in the order they are written.
    fn points() -> Vec<(u64, Vec<Point>)> {
        let mut id = 0;
        CELLS
            .iter()
            .map(|&cell| {
                let children = cell << (2 * (ROW_ORDER - ORDER));
                let rows = (children..children + (1 << (2 * (ROW_ORDER - ORDER))))
                    .map(|child| {
                        let (lon, lat) = cdshealpix::nested::center(ROW_ORDER, child);
                        id += 1;
                        Point {
                            id,
                            ra: lon.to_degrees(),
                            dec: lat.to_degrees(),
                        }
                    })
                    .collect();
                (cell, rows)
            })
            .collect()
    }

    /// A catalog on disk, with a `partition_info.csv` and one parquet file per partition.
    ///
    /// `healpix` is whether the files carry a `_healpix_29` column. Both shapes are real —
    /// the column is a HATS recommendation and not a requirement — and the same query has to
    /// return the same rows either way, since the column is an accelerator and nothing else.
    pub(crate) fn fixture(healpix: bool) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::write(
            root.join("hats.properties"),
            format!(
                "obs_collection=fixture\nhats_col_ra=ra\nhats_col_dec=dec\nhats_order={ORDER}\n"
            ),
        )
        .unwrap();
        let mut csv = String::from("Norder,Npix\n");
        for cell in CELLS {
            csv.push_str(&format!("{ORDER},{cell}\n"));
        }
        fs::write(root.join("partition_info.csv"), csv).unwrap();
        for (cell, rows) in points() {
            let path = root.join(HatsPartition::new(ORDER, cell).path(".parquet"));
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            write_partition(&path, &rows, healpix);
        }
        dir
    }

    fn write_partition(path: &Path, rows: &[Point], healpix: bool) {
        let mut fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("ra", DataType::Float64, false),
            Field::new("dec", DataType::Float64, false),
        ];
        let mut columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.ra).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|row| row.dec).collect::<Vec<_>>(),
            )),
        ];
        if healpix {
            fields.push(Field::new("_healpix_29", DataType::Int64, false));
            columns.push(Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| {
                        let cell =
                            cdshealpix::nested::hash(29, row.ra.to_radians(), row.dec.to_radians());
                        i64::try_from(cell).unwrap()
                    })
                    .collect::<Vec<_>>(),
            )));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let mut writer =
            ArrowWriter::try_new(fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// The catalog directory, addressed the way a request addresses one: by a mount's path.
    fn opened(root: &Path) -> RemoteDir {
        let mounts = Mounts::new(
            &[MountConfig {
                path: MOUNT.to_owned(),
                source: root.display().to_string(),
                serve: false,
                follow_symlinks: false,
                immutable: false,
                filenames: None,
            }],
            &DataConfig::default(),
        )
        .unwrap();
        let policy = AccessPolicy::new(&AccessConfig::default(), Arc::new(mounts)).unwrap();
        crate::storage::open_dir(
            &Url::parse(&format!("file://{MOUNT}/")).unwrap(),
            &StorageOptions::default(),
            &policy,
            &Arc::new(Transfers::new(&LimitsConfig::default())),
        )
        .unwrap()
    }

    fn limits() -> CatalogLimits {
        CatalogLimits::from(&LimitsConfig::default())
    }

    /// The ids the answer came back with, in the order they arrived.
    fn ids(result: &CatalogResult) -> Vec<i64> {
        let mut ids = Vec::new();
        for batch in &result.rows.batches {
            let column = batch.column_by_name("id").unwrap();
            let column = column.as_any().downcast_ref::<Int64Array>().unwrap();
            ids.extend(column.iter().flatten());
        }
        ids
    }

    /// Which rows a shape really holds, worked out from the coordinates rather than from
    /// anything the covering said.
    pub(crate) fn inside(region: &Region) -> Vec<i64> {
        let Region::Circle {
            ra,
            dec,
            radius_deg,
            ..
        } = *region
        else {
            unreachable!("the fixture's regions are circles")
        };
        let radius = radius_deg.unwrap().to_radians();
        let mut ids = Vec::new();
        for (_, rows) in points() {
            for point in rows {
                let separation = (point.dec.to_radians().sin() * dec.to_radians().sin()
                    + point.dec.to_radians().cos()
                        * dec.to_radians().cos()
                        * (point.ra - ra).to_radians().cos())
                .clamp(-1.0, 1.0)
                .acos();
                if separation <= radius {
                    ids.push(point.id);
                }
            }
        }
        ids
    }

    fn cone(ra: f64, dec: f64, radius_deg: f64) -> Region {
        Region::Circle {
            ra,
            dec,
            radius_deg: Some(radius_deg),
            radius_arcsec: None,
        }
    }

    /// A cone somewhere in the middle of the first partition, and one wide enough to take in
    /// the whole fixture.
    pub(crate) fn regions() -> Vec<Region> {
        let (lon, lat) = cdshealpix::nested::center(ORDER, CELLS[0]);
        let (ra, dec) = (lon.to_degrees(), lat.to_degrees());
        // An order-3 cell is about 7 degrees across, so the first of these is well inside
        // one partition, the second crosses into its neighbours, and the third contains
        // every partition the fixture has.
        vec![cone(ra, dec, 1.0), cone(ra, dec, 6.0), cone(ra, dec, 60.0)]
    }

    /// The default bounds are tight enough to refuse the fixture's four partitions, and what
    /// most of these are about is which rows come back rather than what a deployment allows.
    fn generous() -> CatalogLimits {
        CatalogLimits {
            max_partitions: 1_000,
            ..limits()
        }
    }

    async fn read(
        dir: &Path,
        regions: Option<&[Region]>,
        limit: Option<usize>,
    ) -> Result<(Search, CatalogResult), ApiError> {
        match bounded(dir, regions, limit, generous()).await? {
            (search, Outcome::Rows(result)) => Ok((search, *result)),
            (_, Outcome::TooMuchWork(why)) => panic!("unexpectedly refused: {why}"),
        }
    }

    async fn bounded(
        dir: &Path,
        regions: Option<&[Region]>,
        limit: Option<usize>,
        bounds: CatalogLimits,
    ) -> Result<(Search, Outcome), ApiError> {
        let search = Search::resolve(opened(dir), regions, bounds).await?;
        let selection = CatalogSelection {
            regions,
            limit,
            ..CatalogSelection::default()
        };
        let outcome = search
            .run(
                &selection,
                &DataFiles::new(&DataConfig::default().filenames).unwrap(),
                sql::Limits::from(&LimitsConfig::default()),
                bounds,
            )
            .await?;
        Ok((search, outcome))
    }

    /// The rows a region search returns are the rows the region holds — no more, and none
    /// missing — however the catalog was written.
    ///
    /// Everything else here is about cost. This is the one that says the answer is right, and
    /// it asks the question of a fixture with a HEALPix column and one without, because the
    /// column is an accelerator: it changes what a query costs and never which rows come back.
    #[tokio::test]
    async fn a_region_returns_the_rows_it_holds_whether_or_not_the_column_helps() {
        for healpix in [true, false] {
            let dir = fixture(healpix);
            for region in regions() {
                let (_, result) = read(dir.path(), Some(std::slice::from_ref(&region)), None)
                    .await
                    .unwrap();
                assert_eq!(
                    ids(&result),
                    inside(&region),
                    "{region:?} over a catalog {}",
                    match healpix {
                        true => "with a HEALPix column",
                        false => "without one",
                    }
                );
            }
        }
    }

    /// A region narrower than the catalog opens fewer partitions than the catalog has, and a
    /// region that covers it opens all of them.
    ///
    /// This is the step `Coverage::reaches` is for, seen from the outside: not that the right
    /// rows come back, which the test above holds, but that the wrong partitions were never
    /// read to find them.
    #[tokio::test]
    async fn a_narrow_region_opens_fewer_partitions() {
        let dir = fixture(true);
        let opened = async |region| {
            let (search, result) = read(dir.path(), Some(&[region]), None).await.unwrap();
            (search.chosen().len(), result.partitions_read)
        };
        let (narrow, _) = opened(regions()[0].clone()).await;
        let (whole, read_whole) = opened(regions()[2].clone()).await;
        assert_eq!(narrow, 1, "a cone inside one partition reached others");
        assert_eq!(whole, CELLS.len(), "a cone over the fixture missed some");
        assert_eq!(read_whole, CELLS.len());
    }

    /// A region that contains a partition whole leaves its rows no test to meet.
    ///
    /// The covering is computed from both sides for this: the outer set is what makes the
    /// filter correct, and the inner set is what says a partition needs no geometry at all.
    #[tokio::test]
    async fn a_partition_the_region_contains_needs_no_row_test() {
        let dir = fixture(true);
        let (search, _) = read(dir.path(), Some(&[regions()[2].clone()]), None)
            .await
            .unwrap();
        assert!(
            search
                .chosen()
                .iter()
                .any(|chosen| chosen.cover == Cover::Inside),
            "a cone covering the whole fixture contained none of its partitions"
        );
    }

    /// No region is every partition, and every row.
    #[tokio::test]
    async fn a_request_with_no_region_reads_the_whole_catalog() {
        let dir = fixture(true);
        let (search, result) = read(dir.path(), None, None).await.unwrap();
        assert_eq!(search.chosen().len(), CELLS.len());
        let all = points()
            .into_iter()
            .flat_map(|(_, rows)| rows.into_iter().map(|row| row.id))
            .collect::<Vec<_>>();
        assert_eq!(ids(&result), all);
    }

    /// A `limit` takes the front of the catalog in HEALPix order, and takes the same rows
    /// every time.
    ///
    /// It does not stop the read: the partitions go out together, so by the time enough rows
    /// have arrived the rest are already in flight. What a concurrent read must not cost is
    /// *which* rows — a limit answered by whichever partition finished first would return a
    /// different subset each time, and a caller cannot tell that from the data having
    /// changed.
    #[tokio::test]
    async fn a_limit_takes_the_front_of_the_catalog_every_time() {
        let dir = fixture(true);
        let (search, result) = read(dir.path(), None, Some(3)).await.unwrap();
        assert_eq!(search.chosen().len(), CELLS.len());
        assert_eq!(ids(&result), vec![1, 2, 3]);
        for _ in 0..4 {
            let (_, again) = read(dir.path(), None, Some(3)).await.unwrap();
            assert_eq!(ids(&again), vec![1, 2, 3]);
        }
        // A limit past the end is every row and not an error.
        let (_, everything) = read(dir.path(), None, Some(10_000)).await.unwrap();
        assert_eq!(everything.rows.num_rows(), CELLS.len() * 64);
    }

    /// Every bound refuses rather than trims, and says which one it was.
    #[tokio::test]
    async fn each_bound_stops_the_request() {
        let dir = fixture(true);
        let cases = [
            (
                CatalogLimits {
                    max_partitions: 2,
                    ..generous()
                },
                Exceeded::Partitions {
                    reached: 4,
                    allowed: 2,
                },
            ),
            (
                CatalogLimits {
                    max_bytes_fetched: 1,
                    ..generous()
                },
                Exceeded::Bytes {
                    reached: 0,
                    allowed: 1,
                },
            ),
            (
                CatalogLimits {
                    max_rows: 10,
                    ..generous()
                },
                Exceeded::Rows { allowed: 10 },
            ),
        ];
        for (bounds, expected) in cases {
            let (_, outcome) = bounded(dir.path(), None, None, bounds).await.unwrap();
            let Outcome::TooMuchWork(why) = outcome else {
                panic!("{bounds:?} answered with rows");
            };
            // The byte count is whatever the read reached, which is not a fixed number; the
            // other two are the numbers the request turned on.
            match (why, expected) {
                (Exceeded::Bytes { allowed, .. }, Exceeded::Bytes { allowed: want, .. }) => {
                    assert_eq!(allowed, want);
                }
                (found, want) => assert_eq!(found, want),
            }
        }
    }

    /// A catalog that does not name its position columns still answers a request that asks
    /// no spatial question.
    ///
    /// The columns are what a region is tested against, so needing them is the region's
    /// doing and not the catalog's. Refusing every query against such a catalog would refuse
    /// requests that were never going to read a coordinate.
    #[tokio::test]
    async fn a_catalog_with_no_position_columns_still_answers_a_plain_query() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("hats.properties"),
            format!("obs_collection=nameless\nhats_order={ORDER}\n"),
        )
        .unwrap();
        fs::write(
            dir.path().join("partition_info.csv"),
            format!("Norder,Npix\n{ORDER},{}\n", CELLS[0]),
        )
        .unwrap();
        let (cell, rows) = points().into_iter().next().unwrap();
        let path = dir
            .path()
            .join(HatsPartition::new(ORDER, cell).path(".parquet"));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        write_partition(&path, &rows, true);

        let (_, result) = read(dir.path(), None, None).await.unwrap();
        assert_eq!(result.rows.num_rows(), rows.len());

        let error = read(dir.path(), Some(&[regions()[0].clone()]), None)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not name its position columns"),
            "{error}"
        );
    }

    /// An empty region is refused rather than read as "no region at all", which would return
    /// every row of the catalog.
    #[tokio::test]
    async fn an_empty_region_is_refused() {
        let dir = fixture(true);
        let error = read(dir.path(), Some(&[]), None).await.unwrap_err();
        assert_eq!(error.status(), http::StatusCode::BAD_REQUEST);
        assert!(error.to_string().contains("region is empty"), "{error}");
    }
}
