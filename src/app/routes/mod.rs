//! The API's routes, one module per target: [`parquet`] for one file, [`hats`] for a catalog's
//! rows and its plan, [`adql`] for a statement over declared tables, and [`tap`] for the same
//! statement over the tables this service publishes.

pub(in crate::app) mod adql;
pub(in crate::app) mod hats;
pub(in crate::app) mod parquet;
pub(in crate::app) mod tap;
