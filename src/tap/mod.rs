//! IVOA's Table Access Protocol: what this service publishes, and what a query may name.
//!
//! The HTTP surface is `app::routes::tap`. What is here is everything about the protocol
//! that is not a request — the tables an operator declared, and how a name is matched
//! against them.

pub mod metadata;
pub mod schema;
pub mod tables;

pub use metadata::{ColumnMetadata, Marks, TableMetadata};
pub use tables::{TapTable, TapTableList};
