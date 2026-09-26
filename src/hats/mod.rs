//! A HATS catalog: the directory, what it says about itself, and which cells it is cut
//! into.
//!
//! A catalog is addressed as a directory rather than as a file, so nothing here takes a
//! [`crate::storage::RemoteFile`]. What it produces is a [`HatsCatalog`] — what a query needs
//! before it can choose anything to read: the column names, the partition list in HEALPix
//! order, the files inside each partition and the schema, each read when first asked for and
//! kept in [`Catalogs`] for as long as the catalog's [`Lifetime`].
//!
//! **Nothing here decides what to read.** Choosing partitions is `sky::healpix`'s, and
//! running a query against one is `engine::query`'s. This is the part that has to talk to the
//! catalog's own files, and it is kept apart so that the pixel arithmetic stays testable
//! without a catalog and the catalog stays readable without a request.
//!
//! **This service is not a validator.** It reads what it needs to answer the request in
//! front of it and fails on a fault it meets on the way; it does not go looking for one.
//! `CLAUDE.md` says what that rules out.
//!
//! It is also the piece with a life outside this service — the properties file, the
//! partitioning and the `Norder`/`Dir`/`Npix` addressing are the catalog format rather
//! than anything of ours.

pub mod browse;
mod cache;
mod catalog;
mod index;
pub mod partitions;
pub mod properties;
pub mod query;
mod scan;
pub mod table;

pub use cache::{CatalogCache, Catalogs, Lifetime};
pub use catalog::{Columns, HatsCatalog, Partitioned};
pub use partitions::{HatsPartition, HatsPartitionList};
pub use properties::Properties;
pub use scan::OrderByIndex;
