//! The service as a library, so that its parts can be driven from an integration test
//! against a real S3 server rather than only from inside the binary.
//!
//! `main.rs` is the command line, the logging setup and the socket; everything a
//! request touches lives here.

// Every input to this crate comes from a caller: a url, storage options, a projection,
// a predicate. A panic on one of those is a request that takes the process down
// instead of returning a 400, so the panicking shorthands are warned about here rather
// than package-wide — `tests/` is a different crate and wants them.
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    // A row count or a byte offset that silently wraps is a wrong answer, not a crash.
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
// The unit tests inside these modules are held to the ordinary test standard: an
// `unwrap` there fails the test, which is what it is for.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

pub mod access;
pub mod app;
pub mod config;
pub mod data;
pub mod error;
pub mod hats;
pub mod hats_query;
pub mod healpix;
pub mod listing;
pub mod logging;
pub mod materialize;
pub mod mount;
pub mod network;
pub mod parquet_out;
pub mod query;
pub mod region;
pub mod sql;
pub mod storage;
