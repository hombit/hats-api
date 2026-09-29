//! Reading a VOTable: the XML table format IVOA services exchange, and what a TAP upload is
//! written in.
//!
//! The document is walked once, as it is written, and every cell goes straight into an
//! Arrow column: `TABLEDATA`'s text, and the bytes of a `BINARY` or `BINARY2` stream. What
//! a `FIELD` says about its column that Arrow has no place for — the VOTable datatype, the
//! `arraysize`, `unit`, `ucd`, `utype`, `xtype` and the description — is kept in the
//! column's metadata under [`field`]'s keys, which is what lets an answer describe the
//! column the way the caller's document did.
//!
//! Writing one is `output::votable`'s; nothing here is shared with it but those keys.

mod binary;
mod column;
mod datatype;
mod document;
pub mod field;
mod tabledata;

pub use document::{Table, is_votable, read};
