//! The TAP resources, siblings under `{api.prefix}/tap`.

mod answer;
mod examples;
mod format;
mod jobs;
mod page;
mod parameters;
mod published;
mod run;
mod sync;
mod upload;
mod vosi;

#[cfg(test)]
mod tests;

pub(in crate::app) use examples::examples;
pub use jobs::Jobs;
pub(in crate::app) use jobs::{
    act, create, destroy, destruction, error, execution_duration, job_parameters, list, owner,
    phase, quote, result, results, set_destruction, set_execution_duration, set_job_parameters,
    set_phase, show,
};
pub(in crate::app) use page::page;
pub(in crate::app) use sync::{tap_sync_get, tap_sync_post};
pub(in crate::app) use vosi::{availability, capabilities, table, tables};
