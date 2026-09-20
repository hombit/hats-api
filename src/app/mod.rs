//! The HTTP surface: the API's routes, the file server over the mounts, and the router that
//! divides the url space between them.

mod answer;
mod cache;
mod files;
pub mod listing;
pub mod openapi;
mod request;
mod routes;
mod service;
#[cfg(test)]
mod testing;

pub use service::{Service, health_schema, router};
