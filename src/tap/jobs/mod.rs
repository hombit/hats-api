//! The job model behind `/async`: what a job is, what names one, and where they are kept.
//!
//! UWS specifies the shape and this is that shape as data. Running a job is not here — the
//! abort handle and the caller's credentials are process-local, because a restart has no
//! running jobs and a credential must never reach a store.

mod id;
mod job;
mod memory;
mod results;
mod runner;
mod store;

pub use id::JobId;
pub use job::{Change, Job, Phase, Product};
pub use memory::{Bounds, MemoryJobStore};
pub use results::{Results, Written};
pub use runner::{Rendered, Runner, Slots, Work};
pub use store::JobStore;
