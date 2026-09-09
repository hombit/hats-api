//! Which HEALPix cells a catalog is cut into, and how they are found.
//!
//! **Three sources, in priority order**, each used only when the one above it is absent:
//! `partition_info.csv`, then `_metadata`, then a listing of `dataset/`. They answer the
//! same question at different prices — one small `GET`, one large `GET` that also carries
//! sizes, and one `LIST` that not every backend has.
//!
//! **The list is sorted by where each partition begins on the sky**, at the deepest order,
//! which is [`healpix::span`]. Mixed orders nest correctly under it: an order-4 partition
//! sorts among the order-8 ones that would have subdivided it. That sortedness is not
//! cosmetic — it is what lets a region's covering find the partitions it touches with a
//! search rather than a pass over the catalog, and it is the order rows and plan entries
//! come back in.

use std::collections::BTreeMap;
use std::ops::Range;

use datafusion::parquet::file::metadata::ParquetMetaDataReader;

use crate::error::ApiError;
use crate::healpix;
use crate::storage::RemoteDir;

/// Where a catalog's data files live, below the catalog directory.
pub const DATASET_DIR: &str = "dataset";

/// The parquet file whose footer describes every partition's.
pub const METADATA: &str = "dataset/_metadata";

/// The parquet file that carries the schema every partition shares, and no rows.
///
/// Nothing here reads it — a query is planned against the file it is about to read, whose
/// schema is the one that governs. What it is for is naming a catalog's columns without
/// choosing a partition, which is a question the page asks and a query never has to.
pub const COMMON_METADATA: &str = "dataset/_common_metadata";

/// The one-line-per-partition listing at the catalog root.
pub const PARTITION_INFO: &str = "partition_info.csv";

/// One partition: a HEALPix cell, and what the source that found it could say about its
/// size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HatsPartition {
    pub order: u8,
    pub pixel: u64,
    /// Rows in the partition, when the source knew. Only `_metadata` does.
    pub rows: Option<u64>,
    /// Bytes on the wire to read the whole partition — compressed, since that is what a
    /// transfer costs. Only `_metadata` knows it.
    pub bytes: Option<u64>,
}

impl HatsPartition {
    pub fn new(order: u8, pixel: u64) -> Self {
        Self {
            order,
            pixel,
            rows: None,
            bytes: None,
        }
    }

    /// The stretch of the sky this partition holds, in deepest-order cells.
    pub fn span(&self) -> Range<u64> {
        healpix::span(self.order, self.pixel)
    }

    /// Where the partition's file sits below the catalog directory.
    ///
    /// Derived rather than remembered. `partition_info.csv` carries no path at all, so
    /// deriving is the only thing all three sources can agree on — and the layout is the
    /// catalog format's, not a given catalog's. `Dir` groups pixels in ten-thousands, which
    /// is what keeps a directory listable at order 10 and above.
    ///
    /// `suffix` is the catalog's `hats_npix_suffix`. A `/` there means the partition is a
    /// directory of files rather than one file, and this ends in `/` to say so.
    pub fn path(&self, suffix: &str) -> String {
        let directory = self.pixel / 10_000 * 10_000;
        let Self { order, pixel, .. } = self;
        format!("{DATASET_DIR}/Norder={order}/Dir={directory}/Npix={pixel}{suffix}")
    }
}

/// Which of the three sources answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    PartitionInfo,
    Metadata,
    Listing,
}

impl Source {
    pub fn name(self) -> &'static str {
        match self {
            Self::PartitionInfo => PARTITION_INFO,
            Self::Metadata => METADATA,
            Self::Listing => DATASET_DIR,
        }
    }
}

/// A catalog's partitions, sorted and ready to be searched into.
#[derive(Debug, Clone)]
pub struct HatsPartitionList {
    /// Sorted by [`HatsPartition::span`]'s start, and disjoint — which is what
    /// [`Self::overlapping`] relies on.
    cells: Vec<HatsPartition>,
    source: Source,
}

impl HatsPartitionList {
    /// Sort by where each cell begins, and drop a cell named twice.
    ///
    /// The deduplication is not a repair: a cell listed twice is one partition named twice,
    /// which is the same claim and the same file.
    pub(crate) fn new(mut cells: Vec<HatsPartition>, source: Source) -> Self {
        cells.sort_by_key(|cell| (cell.span().start, cell.order));
        cells.dedup_by_key(|cell| (cell.order, cell.pixel));
        Self { cells, source }
    }

    pub fn cells(&self) -> &[HatsPartition] {
        &self.cells
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    pub fn source(&self) -> Source {
        self.source
    }

    /// The deepest order any partition is at, which is what a covering for choosing
    /// partitions is built against.
    ///
    /// Taken from the partitions rather than from `hats_order`, because this is the list
    /// that decides what gets read: a catalog whose properties disagree with its own files
    /// would otherwise have a covering built for a resolution it does not have.
    pub fn order(&self) -> Option<u8> {
        self.cells.iter().map(|cell| cell.order).max()
    }

    /// The partitions overlapping a stretch of sky, as a contiguous slice.
    ///
    /// Two binary searches, which is the point of keeping the list sorted. A region's
    /// covering has tens of ranges and a catalog has hundreds of thousands of partitions,
    /// so asking each range which partitions it touches costs `R log P` — against `R + P`
    /// for a merge, which cannot skip, and against `P` classifications for a pass over the
    /// catalog.
    ///
    /// Both bounds take the partitions for a tiling — disjoint, which is what a catalog's
    /// partitions are. Nothing checks it: a catalog whose cells nest gets a slice that may
    /// leave one out, the way anything reading a broken catalog gets a broken answer.
    pub fn overlapping(&self, range: &Range<u64>) -> &[HatsPartition] {
        let from = self
            .cells
            .partition_point(|cell| cell.span().end <= range.start);
        let to = self
            .cells
            .partition_point(|cell| cell.span().start < range.end);
        self.cells.get(from..to.max(from)).unwrap_or_default()
    }
}

/// Find a catalog's partitions, trying each source until one answers.
///
/// A source that is absent is a source that was not tried; a source that is present and
/// unreadable is an error, since a catalog carrying a `partition_info.csv` of nonsense is
/// saying something wrong rather than saying nothing. The exception is `_metadata` being
/// too large, which is a size this service declines to fetch rather than a fault in the
/// file — that one falls through.
pub async fn discover(
    dir: &RemoteDir,
    max_metadata_bytes: u64,
) -> Result<HatsPartitionList, ApiError> {
    if let Some(bytes) = dir.read_if_present(PARTITION_INFO).await? {
        return Ok(HatsPartitionList::new(
            from_partition_info(&bytes)?,
            Source::PartitionInfo,
        ));
    }
    if let Some(cells) = read_metadata(dir, max_metadata_bytes).await? {
        return Ok(HatsPartitionList::new(cells, Source::Metadata));
    }
    let listing = dir.list(DATASET_DIR).await.map_err(|error| {
        tracing::debug!(%error, "cannot list a catalog's dataset directory");
        ApiError::bad_request(format!(
            "this catalog has no {PARTITION_INFO} and no readable {METADATA}, and its \
             partitions cannot be listed either: {error}"
        ))
    })?;
    let cells = listing
        .iter()
        .filter_map(|entry| cell_from_path(&entry.name))
        .collect();
    Ok(HatsPartitionList::new(cells, Source::Listing))
}

/// `Norder,Npix`, and whatever else a writer put beside them.
///
/// The two columns are found by name rather than by position: the header is what the
/// format specifies, and a writer that adds a third column ahead of them is not a writer
/// whose catalog should be misread a column at a time.
fn from_partition_info(bytes: &[u8]) -> Result<Vec<HatsPartition>, ApiError> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(bytes);
    let headers = reader
        .headers()
        .map_err(|error| malformed(PARTITION_INFO, &error))?
        .clone();
    let column = |name: &str| {
        headers
            .iter()
            .position(|header| header.eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                ApiError::bad_request(format!(
                    "this catalog's {PARTITION_INFO} has no {name} column; it has {}",
                    headers.iter().collect::<Vec<_>>().join(", ")
                ))
            })
    };
    let (norder, npix) = (column("Norder")?, column("Npix")?);
    let mut cells = Vec::new();
    for record in reader.records() {
        let record = record.map_err(|error| malformed(PARTITION_INFO, &error))?;
        let field = |at: usize| -> Result<u64, ApiError> {
            let raw = record.get(at).unwrap_or_default();
            raw.parse().map_err(|error| {
                ApiError::bad_request(format!(
                    "this catalog's {PARTITION_INFO} has {raw:?} where a cell number was \
                     expected: {error}"
                ))
            })
        };
        cells.push(cell(field(norder)?, field(npix)?)?);
    }
    Ok(cells)
}

/// The footer of `_metadata`, which describes every partition's row groups.
///
/// `None` is a catalog with no `_metadata`, or one whose `_metadata` is larger than this
/// service will fetch. Both mean "ask the next source".
async fn read_metadata(
    dir: &RemoteDir,
    max_bytes: u64,
) -> Result<Option<Vec<HatsPartition>>, ApiError> {
    let Some(size) = dir.size(METADATA).await? else {
        return Ok(None);
    };
    // The file is all footer — `_metadata` holds no rows — so reading it whole is reading
    // its metadata, and a ranged footer read would only turn one request into two.
    if size > max_bytes {
        tracing::warn!(
            size,
            max_bytes,
            "a catalog's {METADATA} is larger than limits.max_catalog_metadata_bytes; \
             falling back to listing its partitions"
        );
        return Ok(None);
    }
    let bytes = dir.read(METADATA).await?;
    Ok(Some(from_metadata(&bytes)?))
}

fn from_metadata(bytes: &bytes::Bytes) -> Result<Vec<HatsPartition>, ApiError> {
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(bytes)
        .map_err(ApiError::SourceMetadata)?;
    // A partition may be written as several row groups, and `_metadata` carries them all,
    // so the rows and bytes accumulate per file rather than being read off one group.
    let mut totals: BTreeMap<(u8, u64), (u64, u64)> = BTreeMap::new();
    for group in metadata.row_groups() {
        // The path is a property of the column chunk in parquet's own model; every chunk
        // of one row group carries the same one.
        let path = group.columns().first().and_then(|chunk| chunk.file_path());
        let Some(cell) = path.and_then(cell_from_path) else {
            continue;
        };
        let totals = totals.entry((cell.order, cell.pixel)).or_default();
        totals.0 = totals
            .0
            .saturating_add(group.num_rows().try_into().unwrap_or(0));
        totals.1 = totals
            .1
            .saturating_add(group.compressed_size().try_into().unwrap_or(0));
    }
    Ok(totals
        .into_iter()
        .map(|((order, pixel), (rows, bytes))| HatsPartition {
            order,
            pixel,
            rows: Some(rows),
            bytes: Some(bytes),
        })
        .collect())
}

/// The cell a path names, from its `Norder=` and `Npix=` components.
///
/// Read out of the path's own components rather than matched against a shape, so that the
/// same function reads a listing entry, a `_metadata` `file_path` and a partition whose
/// `Npix=` is a directory. A path missing either is not a partition — `_common_metadata`
/// and a catalog's `point_map.fits` sit in the same tree — and is skipped rather than
/// refused.
fn cell_from_path(path: &str) -> Option<HatsPartition> {
    let (mut order, mut pixel) = (None, None);
    for component in path.split('/') {
        let Some((key, value)) = component.split_once('=') else {
            continue;
        };
        let found = match key {
            "Norder" => &mut order,
            "Npix" => &mut pixel,
            _ => continue,
        };
        // The component carries the file's extension with it — `Npix=12.parquet` — so the
        // number is the digits it begins with. A component beginning with none of them
        // leaves this `None`, which is a component shaped like a cell and naming none.
        let digits = value
            .split(|c: char| !c.is_ascii_digit())
            .next()
            .unwrap_or_default();
        *found = digits.parse::<u64>().ok();
    }
    cell(order?, pixel?).ok()
}

/// A cell, refused unless the order is one HEALPix has and the pixel is one that order
/// has.
///
/// A pixel outside its order is not a partition that reads as empty — it is a partition
/// whose span would be computed from arithmetic that overflowed, so it would claim a
/// stretch of sky belonging to a real partition and shadow it in every search.
fn cell(order: u64, pixel: u64) -> Result<HatsPartition, ApiError> {
    let order = u8::try_from(order)
        .ok()
        .filter(|order| *order <= healpix::MAX_ORDER)
        .ok_or_else(|| {
            ApiError::bad_request(format!(
                "this catalog names a partition at HEALPix order {order}, and the deepest \
                 order is {}",
                healpix::MAX_ORDER
            ))
        })?;
    if pixel >= cdshealpix::nested::n_hash(order) {
        return Err(ApiError::bad_request(format!(
            "this catalog names pixel {pixel} at HEALPix order {order}, which has {} cells",
            cdshealpix::nested::n_hash(order)
        )));
    }
    Ok(HatsPartition::new(order, pixel))
}

fn malformed(file: &str, error: &dyn std::fmt::Display) -> ApiError {
    ApiError::bad_request(format!("this catalog's {file} cannot be read: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cells(pairs: &[(u8, u64)]) -> HatsPartitionList {
        HatsPartitionList::new(
            pairs
                .iter()
                .map(|(order, pixel)| HatsPartition::new(*order, *pixel))
                .collect(),
            Source::Listing,
        )
    }

    #[test]
    fn partition_info_is_read_by_column_name() {
        let list = from_partition_info(b"Norder,Npix\n0,11\n3,264\n").unwrap();
        assert_eq!(
            list,
            vec![HatsPartition::new(0, 11), HatsPartition::new(3, 264)]
        );
        // And in whatever order a writer put the columns, beside whatever else.
        let swapped = from_partition_info(b"Npix, Dir, Norder\n264, 0, 3\n").unwrap();
        assert_eq!(swapped, vec![HatsPartition::new(3, 264)]);
    }

    #[test]
    fn a_partition_list_that_cannot_be_read_is_refused_rather_than_emptied() {
        for file in [
            &b"Norder,Nside\n0,11\n"[..],
            &b"Norder,Npix\n0,eleven\n"[..],
            &b"Norder,Npix\n99,11\n"[..],
            &b"Norder,Npix\n0,12\n"[..],
        ] {
            assert!(from_partition_info(file).is_err(), "{file:?}");
        }
    }

    /// The path is the one thing all three sources spell differently, and the same
    /// function has to read all three.
    #[test]
    fn a_cell_is_read_out_of_a_path_however_the_partition_is_written() {
        for path in [
            "Norder=3/Dir=0/Npix=264.parquet",
            "Norder=3/Dir=0/Npix=264.parq",
            "Norder=3/Dir=0/Npix=264/part0.parquet",
            "dataset/Norder=3/Dir=0/Npix=264.parquet",
        ] {
            assert_eq!(
                cell_from_path(path),
                Some(HatsPartition::new(3, 264)),
                "{path}"
            );
        }
        for path in [
            "_common_metadata",
            "Norder=3/Dir=0",
            "point_map.fits",
            // A component shaped like one and carrying no number at all.
            "Norder=3/Npix=part0.parquet",
        ] {
            assert_eq!(cell_from_path(path), None, "{path}");
        }
    }

    /// Derived from the cell, because `partition_info.csv` carries no path to remember.
    #[test]
    fn a_partition_names_its_own_file() {
        assert_eq!(
            HatsPartition::new(5, 12_240).path(".parquet"),
            "dataset/Norder=5/Dir=10000/Npix=12240.parquet"
        );
        assert_eq!(
            HatsPartition::new(0, 11).path("/"),
            "dataset/Norder=0/Dir=0/Npix=11/"
        );
    }

    /// Sorted by where each partition begins on the sky, which for mixed orders is not the
    /// order the numbers sort in and not the order the names sort in either.
    #[test]
    fn partitions_come_out_in_healpix_order() {
        let list = cells(&[(1, 5), (0, 0), (3, 100), (0, 2), (1, 4)]);
        let spelled: Vec<_> = list
            .cells()
            .iter()
            .map(|cell| (cell.order, cell.pixel))
            .collect();
        // Order-1 pixels 4 and 5 are inside order-0 pixel 1, so they sort between order-0
        // pixels 0 and 2; order-3 pixel 100 is inside order-1 pixel 6, after both.
        assert_eq!(spelled, vec![(0, 0), (1, 4), (1, 5), (3, 100), (0, 2)]);
    }

    #[test]
    fn a_stretch_of_sky_selects_a_contiguous_run_of_partitions() {
        let list = cells(&[(2, 0), (2, 1), (2, 2), (2, 3), (2, 4)]);
        let span = |pixel| healpix::span(2, pixel);

        // A range covering one cell exactly selects it and nothing beside it.
        assert_eq!(list.overlapping(&span(2)), &list.cells()[2..3]);
        // One that touches two, by a single deepest-order cell at each end.
        let across = span(1).start + 1..span(3).start + 1;
        assert_eq!(list.overlapping(&across), &list.cells()[1..4]);
        // And one that lands where the catalog has nothing.
        assert!(list.overlapping(&(span(4).end..span(4).end + 1)).is_empty());
    }

    /// Not the properties file's `hats_order`: this is the list that says what gets read.
    #[test]
    fn the_catalogs_order_is_its_deepest_partition() {
        // One cell from each of three base cells, so the mixed orders do not overlap.
        assert_eq!(cells(&[(0, 0), (5, 1100), (3, 130)]).order(), Some(5));
        assert_eq!(cells(&[]).order(), None);
    }

    /// One partition named twice, which is one partition.
    #[test]
    fn a_cell_named_twice_is_one_partition() {
        assert_eq!(cells(&[(3, 264), (3, 264)]).len(), 1);
    }
}
