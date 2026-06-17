mod config;
mod file;
mod fs;

mod local_state;
pub mod pipeline;
#[cfg(test)]
mod tests;

use std::ops::Range;

pub use config::DiskCacheConfig;
pub use file::DiskCache;
pub use fs::{DiskCacheFs, DiskCacheFsContext};

use crate::universal_io::{OwningPipeline, UniversalRead, UniversalReadFs};

/// The file-owning remote pipeline used to prefill a [`DiskCache`] from its
/// remote. Derived generically from the remote's single
/// [`ReadPipeline`](UniversalRead::ReadPipeline).
pub(super) type OwnedRemotePipeline<R, U> =
    OwningPipeline<<R as UniversalRead>::ReadPipeline<'static, U>, R, U>;

/// Trait bundle for remote backends that can be cached by [`DiskCache`].
///
/// The `ReadPipeline<'static, ()>: Send` and `Send` supertrait bounds together
/// make [`OwnedRemotePipeline<Self, ()>`] `Send`, as required to store the
/// prefiller inside the shared [`DiskCache`].
pub trait DiskCacheRemote:
    UniversalRead<
        Fs: Clone + Send + Sync + UniversalReadFs<OpenExtra: Clone + Send + Sync>,
        ReadPipeline<'static, ()>: Send,
    > + Clone
    + Send
    + 'static
{
}

impl<R> DiskCacheRemote for R
where
    R: UniversalRead<ReadPipeline<'static, ()>: Send> + Clone + Send + 'static,
    R::Fs: Clone + Send + Sync,
    <R::Fs as UniversalReadFs>::OpenExtra: Clone + Send + Sync,
{
}

/// Files are logically split into fixed-size blocks; the roaring bitmap
/// tracks population on a per-block basis.
///
/// Matches `disk_cache::BLOCK_SIZE` and is a small multiple of typical
/// filesystem block sizes (usually 4 KiB).
const BLOCK_SIZE: usize = 16 * 1024; // 16kB

fn to_block_range(byte_range: Range<u64>) -> Range<u32> {
    let start = (byte_range.start / BLOCK_SIZE as u64) as u32;
    if byte_range.start >= byte_range.end {
        // empty byte range returns empty block range
        return start..start;
    }
    let end = byte_range.end.div_ceil(BLOCK_SIZE as u64) as u32;
    start..end
}
