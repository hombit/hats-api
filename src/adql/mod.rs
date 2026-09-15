//! IVOA's ADQL: `translate` rewrites a statement into the SQL DataFusion plans, [`query`] runs
//! one against the tables a request declared, and [`functions`] registers what the language
//! requires and DataFusion has not got.

pub mod functions;
pub mod query;
mod translate;

pub use translate::{Translated, translate};
