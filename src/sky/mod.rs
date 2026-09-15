//! A shape on the sky: [`region`] says what one means as a row predicate, [`healpix`] which
//! cells it covers, and [`geometry`] lets a statement say one as a function.

pub mod geometry;
pub mod healpix;
pub mod region;
