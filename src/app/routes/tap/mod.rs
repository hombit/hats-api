//! The TAP resources, siblings under `{api.prefix}/tap`.

mod answer;
mod format;
mod parameters;
mod published;
mod sync;
mod vosi;

#[cfg(test)]
mod tests;

pub(in crate::app) use sync::{tap_sync_get, tap_sync_post};
pub(in crate::app) use vosi::{availability, capabilities, table, tables};
