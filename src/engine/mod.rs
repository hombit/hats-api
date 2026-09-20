//! What a caller's request becomes once it is SQL: [`sql`] decides what an expression may
//! say, and [`query`] runs a selection against one parquet file.

pub mod query;
pub mod sql;
pub mod whole;
