//! Helpers shared between integration tests.
//!
//! Files in `tests/` are each their own crate, but a *subdirectory* is not compiled as a
//! test target — so this is the standard place for code two test binaries both need.

#![allow(dead_code)] // each test binary uses a different subset

pub mod offline;
