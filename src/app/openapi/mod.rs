//! The API's own description, and a page that renders it.
//!
//! The schemas come from the same types the routes deserialize, so a field added to a
//! request appears here by compiling rather than by being remembered. The paths do not: they
//! are registered beside the routes ([`crate::app::router`]), which is what keeps a route from
//! being served and undescribed.
//!
//! It describes API mode only. The file-server mode has no route set to enumerate — every
//! url below a mount is a data path — so OpenAPI would have to invent a shape for it.
//!
//! `description` is what this service's own routes say; `document` builds the document around
//! them, and `page` renders it.

pub(in crate::app) mod description;
mod document;
mod page;

pub(in crate::app) use document::{document, health, operation, post};
pub(in crate::app) use page::page;
