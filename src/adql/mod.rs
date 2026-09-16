//! IVOA's ADQL: `translate` rewrites a statement into the SQL DataFusion plans, [`query`] runs
//! one against the tables a request declared, [`functions`] registers what the language
//! requires and DataFusion has not got, [`names`] says how a name is written in it, and
//! [`language`] which version a request may say it wrote.

pub mod functions;
pub mod language;
pub mod names;
pub mod query;
mod translate;

pub use translate::{Translated, translate};
