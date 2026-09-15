//! The API's routes, one module per target: [`parquet`] for one file, [`hats`] for a catalog's
//! rows and its plan, and [`adql`] for a statement over declared tables.

pub(in crate::app) mod adql;
pub(in crate::app) mod hats;
pub(in crate::app) mod parquet;
