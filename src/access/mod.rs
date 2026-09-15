//! What a request is allowed to read: which endpoint it may name (`policy`), which address
//! that resolves to ([`network`]), which directories are readable at all ([`mount`]) and what
//! a path under one may reach ([`local`]), and which files under any of them are data
//! ([`data`]).

pub mod data;
pub mod local;
pub mod mount;
pub mod network;
mod policy;

pub use policy::{
    AccessPolicy, BACKENDS, Backend, EndpointScheme, LOCAL_SCHEME, Target,
    describe_endpoint_schemes,
};
