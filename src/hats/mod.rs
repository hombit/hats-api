//! A HATS catalog: the directory, what it says about itself, and which cells it is cut
//! into.
//!
//! A catalog is addressed as a directory rather than as a file, so nothing here takes a
//! [`crate::storage::RemoteFile`]. What it produces is a [`Catalog`] — the two things a
//! spatial query needs before it can choose anything to read: the column names, and the
//! partition list in HEALPix order.
//!
//! **Nothing here decides what to read.** Choosing partitions is `healpix.rs`'s, and
//! running a query against one is `query.rs`'s. This is the part that has to talk to the
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

mod catalog;
pub mod partitions;
pub mod properties;

pub use catalog::{Catalog, Columns, Partitioned};
pub use partitions::{Partition, Partitions};
pub use properties::Properties;
