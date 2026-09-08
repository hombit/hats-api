//! Opening a catalog: which of its own files describe it, and where its partitions are.

use crate::data::DataFiles;
use crate::error::ApiError;
use crate::storage::{RemoteDir, RemoteFile};

use super::partitions::{self, Partition, Partitions};
use super::properties::{self, Properties};

/// An opened catalog.
#[derive(Debug)]
pub struct Catalog {
    dir: RemoteDir,
    properties: Properties,
    partitions: Partitions,
    collection: Option<Properties>,
}

impl Catalog {
    /// Read a catalog's own files: `properties`, then its partition list.
    ///
    /// Two `GET`s for the ordinary catalog, which is one for each. Nothing is cached
    /// between requests yet, so this is the cost of every query against a catalog.
    ///
    /// `hats.properties`, then the deprecated `properties`, then `collection.properties` —
    /// so a catalog is described by the first request and only a collection pays for the
    /// other two. A collection opens as the catalog it calls its primary table, which it
    /// may only name as a path inside itself.
    pub async fn open(dir: RemoteDir, max_catalog_metadata_bytes: u64) -> Result<Self, ApiError> {
        let (dir, properties, collection) = match read_properties(&dir).await? {
            Some(properties) => (dir, properties, None),
            None => {
                let collection = read_collection(&dir).await?.ok_or_else(not_a_catalog)?;
                let inside = dir.subdir(primary_table(&collection)?)?;
                let properties = read_properties(&inside).await?.ok_or_else(not_a_catalog)?;
                (inside, properties, Some(collection))
            }
        };
        let partitions = partitions::discover(&dir, max_catalog_metadata_bytes).await?;
        Ok(Self {
            dir,
            properties,
            partitions,
            collection,
        })
    }

    pub fn dir(&self) -> &RemoteDir {
        &self.dir
    }

    /// The collection this catalog was reached through, if it was.
    ///
    /// What it carries beyond the primary table — `all_margins`, `default_margin`,
    /// `all_indexes` — is not read yet, and each of those is a catalog of its own.
    pub fn collection(&self) -> Option<&Properties> {
        self.collection.as_ref()
    }

    pub fn properties(&self) -> &Properties {
        &self.properties
    }

    pub fn partitions(&self) -> &Partitions {
        &self.partitions
    }

    /// The deepest order the catalog is partitioned at, which is what a covering for
    /// choosing partitions is sized against.
    ///
    /// The partition list, not `hats_order`: this is the list a query is answered from, so
    /// a covering built for a resolution the files do not have would be built for nothing.
    pub fn order(&self) -> Option<u8> {
        self.partitions.order()
    }

    /// Where a partition's rows are, which is one file or a directory of them.
    ///
    /// `hats_npix_suffix` decides which, and `/` — a directory — is what a catalog large
    /// enough to split a partition writes. It is not a rare shape: ZTF DR24's object
    /// catalog is one. So nothing may assume a partition is a single object.
    pub fn partition(&self, partition: &Partition) -> Result<Partitioned, ApiError> {
        let suffix = self.properties.npix_suffix();
        let path = partition.path(suffix);
        match self.properties.partition_is_a_directory() {
            false => Ok(Partitioned::One(self.dir.child(&path)?)),
            true => Ok(Partitioned::Many(self.dir.subdir(&path)?)),
        }
    }

    /// The coordinate columns and the HEALPix column, from the catalog's properties.
    ///
    /// This is what makes `ra_column` and `dec_column` optional against a catalog and
    /// required against a lone parquet file: a file says nothing about which of its columns
    /// are a position, and a catalog says exactly that.
    pub fn columns(&self) -> Result<Columns<'_>, ApiError> {
        let (ra, dec) = self.properties.coordinate_columns().ok_or_else(|| {
            ApiError::bad_request(
                "this catalog does not say which of its columns hold a position, so a \
                 region search against it must name ra_column and dec_column",
            )
        })?;
        Ok(Columns {
            ra,
            dec,
            healpix: self.properties.healpix_column()?,
        })
    }
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
fn primary_table(collection: &Properties) -> Result<&str, ApiError> {
    let refuse = |why: &str| {
        ApiError::bad_request(format!(
            "this collection's hats_primary_table_url {why}; this service follows a \
             collection only to a catalog inside it, so name that catalog's own url"
        ))
    };
    let named = collection
        .get("hats_primary_table_url")
        .ok_or_else(|| refuse("is missing"))?;
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
    Ok(named)
}

/// A partition's rows, as one file or as a directory holding several.
#[derive(Debug)]
pub enum Partitioned {
    One(RemoteFile),
    Many(RemoteDir),
}

impl Partitioned {
    /// The files to read.
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
    pub async fn files(&self, data: &DataFiles) -> Result<Vec<RemoteFile>, ApiError> {
        match self {
            Self::One(file) => Ok(vec![file.clone_handle()]),
            Self::Many(dir) => {
                let mut files = Vec::new();
                for entry in dir.list("").await? {
                    if data.matches(entry.name.rsplit('/').next().unwrap_or(&entry.name)) {
                        files.push(dir.child(&entry.name)?);
                    }
                }
                Ok(files)
            }
        }
    }
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
    use std::sync::Arc;

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
    use crate::materialize::Transfers;
    use crate::storage::StorageOptions;

    /// The cells `small_sky_order3_source` is cut into, cut down to a handful. Mixed with
    /// a coarse one, which is what a real catalog does where the sky is empty.
    const CELLS: [(u8, u64); 4] = [(3, 264), (3, 707), (1, 43), (3, 708)];

    fn opened(root: &Path, max_metadata_bytes: u64) -> Result<Catalog, ApiError> {
        let policy = AccessPolicy::new(
            &AccessConfig {
                local: crate::config::LocalConfig {
                    paths: vec![root.display().to_string()],
                    follow_symlinks: false,
                },
                ..Default::default()
            },
            &crate::mount::Mounts::default(),
        )
        .unwrap();
        // macOS puts a temporary directory behind a symlink, and the policy matches the
        // canonical path it resolved the configured root to.
        let url = Url::from_directory_path(root.canonicalize().unwrap()).unwrap();
        let dir = crate::storage::open_dir(
            &url,
            &StorageOptions::default(),
            &policy,
            &Arc::new(Transfers::new(&LimitsConfig::default())),
        )?;
        block_on(Catalog::open(dir, max_metadata_bytes))
    }

    fn open(root: &Path) -> Catalog {
        opened(root, u64::MAX).unwrap()
    }

    /// A catalog with `properties` and a `dataset/` tree, and whichever of the two
    /// partition sources the case wants.
    fn catalog(properties: &str, partition_info: bool, metadata: bool) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join("hats.properties"), properties).unwrap();
        if partition_info {
            let mut csv = String::from("Norder,Npix\n");
            for (order, pixel) in CELLS {
                csv.push_str(&format!("{order},{pixel}\n"));
            }
            fs::write(root.join(partitions::PARTITION_INFO), csv).unwrap();
        }
        let mut written = Vec::new();
        for (order, pixel) in CELLS {
            let cell = Partition::new(order, pixel);
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

    fn write_partition(path: &Path, rows: usize) -> ParquetMetaData {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ra",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(vec![1.0; rows]))],
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
            let catalog = open(dir.path());
            assert_eq!(catalog.partitions().source(), expected);
            spellings.push(
                catalog
                    .partitions()
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
        let sized = open(dir.path());
        for cell in sized.partitions().cells() {
            assert_eq!(cell.rows, Some(cell.pixel % 7 + 1), "{cell:?}");
            assert!(cell.bytes.is_some_and(|bytes| bytes > 0), "{cell:?}");
        }

        let dir = catalog(PROPERTIES, true, true);
        let cheap = open(dir.path());
        for cell in cheap.partitions().cells() {
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
        assert_eq!(open(dir.path()).order(), Some(3));
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
        let listed = opened(dir.path(), 1).unwrap();
        assert_eq!(listed.partitions().source(), partitions::Source::Listing);
        assert_eq!(listed.partitions().len(), CELLS.len());
    }

    /// Derived from the cell and the catalog's own suffix, and it has to name a file that
    /// is really there — the `Dir=` grouping is the part a reader gets wrong.
    #[test]
    fn a_partition_url_names_the_file_that_holds_it() {
        let dir = catalog(PROPERTIES, true, false);
        let opened = open(dir.path());
        let data = DataFiles::default();
        for cell in opened.partitions().cells() {
            let files = block_on(opened.partition(cell).unwrap().files(&data)).unwrap();
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

    /// `hats_npix_suffix=/` makes a partition a directory of files, which is what a catalog
    /// large enough to split one writes — ZTF DR24's objects among them. Every file in it
    /// is read, and what in it counts as data is the configured list's answer.
    #[test]
    fn a_partition_may_be_a_directory_of_files() {
        let dir = catalog(&format!("{PROPERTIES}hats_npix_suffix=/\n"), true, false);
        // Rewrite the tree the way such a catalog is written: `Npix=p/` holding parts.
        for (order, pixel) in CELLS {
            let one = dir
                .path()
                .join(Partition::new(order, pixel).path(".parquet"));
            let many = dir.path().join(Partition::new(order, pixel).path("/"));
            fs::create_dir_all(&many).unwrap();
            fs::rename(&one, many.join("part0.parquet")).unwrap();
            fs::copy(many.join("part0.parquet"), many.join("part1.parquet")).unwrap();
            fs::write(many.join("_SUCCESS"), b"").unwrap();
        }

        let opened = open(dir.path());
        let data = DataFiles::default();
        for cell in opened.partitions().cells() {
            let files = block_on(opened.partition(cell).unwrap().files(&data)).unwrap();
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
        assert_eq!(opened.partitions().len(), CELLS.len());
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
}
