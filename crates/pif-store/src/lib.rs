//! The local block archive that sits between fetching a block and processing it.
//!
//! PIF used to fetch and process a block in one indivisible step, so a re-index cost a
//! re-download. This crate is the half that makes those two separable: raw blocks land in
//! segment files on local disk, and the digest stage reads from there rather than from the
//! network. Adding a handler, or fixing a decode bug, becomes a local operation over bytes
//! you already have.
//!
//! Two properties are load-bearing and easy to lose:
//!
//! * **Blocks are archived raw, never decoded.** Decoding is the part most likely to have
//!   bugs and the part a replay exists to redo. Runtime metadata is archived alongside, once
//!   per `spec_version`, so a block can be decoded years later by whatever decoder is
//!   current.
//! * **Only finalized blocks belong here.** The archive is keyed by block *number*, which is
//!   unique only because a finalized block cannot be reverted. Anything that later archives
//!   unfinalized blocks gets a silent key collision rather than an error.
//!
//! Two stores live under one root, keyed the same way and holding different things:
//!
//! * [`HotStore`] — the blocks themselves, plus runtime metadata per `spec_version`.
//! * [`StorageCache`] — the answers handlers got when they read chain **state** at a block.
//!   A block archive is not a state archive: `pallet_identity` emits `IdentitySet { who }`
//!   with no display name, so archiving blocks alone would move the RPC load onto historical
//!   state reads rather than removing it.
//!
//! Both can be opened over **two** roots: a hot one they are written to, and a cold one that
//! digested segments are moved to once they have been on the SSD long enough ([`cold`]).
//! Reads fall through, so history that has moved to an HDD is still replayable — at a tenth
//! of the price per byte, and with no caller aware of which tier answered.
//!
//! This crate owns bytes on disk and nothing else: no Postgres, no subxt, no knowledge of
//! the traits or error types above it. That is a compile requirement rather than a
//! preference — see IPD-002 §11.1.1.

pub mod cache;
pub mod cold;
pub mod error;
pub mod hot;
pub mod layout;
pub mod raw;
pub mod segment;
mod segstore;

pub use cache::{BlockReads, ReadKey, StorageCache};
pub use cold::{SegmentSpan, Tierable, Tiered};
pub use error::{Result, StoreError};
pub use hot::{HotStore, StoreUsage};
pub use layout::DEFAULT_SEGMENT_SIZE;
pub use raw::RawBlock;
pub use segstore::Usage;
