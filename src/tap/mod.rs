//! IVOA's Table Access Protocol: what this service publishes, and what a query may name.
//!
//! The HTTP surface is `app::routes::tap`. What is here is everything about the protocol
//! that is not a request — the tables an operator declared, how a name is matched against
//! them, and the grammar a parameter's value is written in.

pub mod dali;
pub mod jobs;
pub mod metadata;
pub mod schema;
pub mod tables;
pub mod uws;

pub use metadata::{ColumnMetadata, Marks, TableMetadata};
pub use tables::{TapExample, TapTable, TapTableList};
