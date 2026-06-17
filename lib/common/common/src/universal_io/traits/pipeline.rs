//! Read pipelines.
//!
//! Workflow:
//! 1. Push read operations using `schedule()`.
//! 2. Pull the completed results using `wait()`.
//! 3. Interleave steps 1 and 2 as needed.
//!
//! Each backend implements a single [`ReadPipelineImpl`]: the *borrowed* shape,
//! where every read is scheduled against an externally-held `&'file File` and
//! [`wait`](ReadPipelineImpl::wait) yields data bounded by that `'file`.
//!
//! The owned shape — a pipeline that *owns* its file so it can live in a
//! long-lived structure independent of any file borrow — is provided generically
//! by [`OwningPipeline`] on top of the same impl. It is the only place the
//! self-referential `unsafe` lives.
//!
//! ## On the `'file` lifetime
//!
//! For backends whose reads borrow from the file (mmap, disk cache) `'file`
//! genuinely bounds the returned slice. For backends whose reads produce *owned*
//! buffers (`io_uring`, object store) `'file` never bounds returned data — it is
//! a pure *file-safety guard* ensuring the file (and any fd / runtime
//! referencing it) outlives all in-flight operations.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ops::Range;

use super::{Item, UniversalRead};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::{AccessPattern, Sequential};
use crate::universal_io::{Result, UserData};

/// The single read-pipeline shape implemented **once** per backend.
///
/// Reads are scheduled against an externally-held `&'file File`; results from
/// [`wait`](Self::wait) are bounded by `'file` (i.e. they may outlive the
/// pipeline itself, but not the file). See the [module docs](self) for the
/// meaning of `'file` across backends, and [`OwningPipeline`] for the
/// file-owning variant.
pub trait ReadPipelineImpl<'file, U>: Sized
where
    U: UserData,
{
    type File: 'file;

    fn new() -> Result<Self>;

    fn can_schedule(&mut self) -> bool;

    /// Schedule a read operation.
    ///
    /// An implementation might add it to an internal queue, but not actually
    /// execute it until [`wait`](Self::wait) is called.
    ///
    /// Should be called only when [`can_schedule`](Self::can_schedule) is
    /// `true`. Returns [`UniversalIoError::QueueIsFull`](crate::universal_io::UniversalIoError::QueueIsFull)
    /// otherwise.
    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file Self::File,
        range: Range<u64>,
        align: usize,
    ) -> Result<()>;

    /// Block until any scheduled operation completes and consume its result.
    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>>;

    #[inline]
    fn wait_bytemuck<T: Item>(&mut self) -> Result<Option<(U, Cow<'file, [T]>)>> {
        let Some((user_data, bytes)) = self.wait()? else {
            return Ok(None);
        };
        Ok(Some((user_data, bytes.try_cast_bytemuck().unwrap())))
    }
}

/// File-owning adapter over a [`ReadPipelineImpl`].
///
/// Owns the file so the pipeline can be held in long-lived structures with no
/// file borrow. `I` must be the impl at the `'static` lifetime
/// (`I::File = F`); the impl borrows the owned file's stable address, which is
/// sound because [`Drop`] guarantees the impl (holding any in-flight
/// references) is dropped *before* the file.
///
/// `wait` returns `ACow<'_>` (bound to `&mut self`): the result might outlive
/// the file, but not the pipeline.
pub struct OwningPipeline<I, F, U> {
    // Drop order matters: `impl_` (may hold in-flight ops referencing `file`)
    // must be dropped before `file`. Enforced by the manual `Drop` below.
    impl_: ManuallyDrop<I>,
    file: ManuallyDrop<F>,
    _user_data: PhantomData<fn() -> U>,
}

impl<U, I, F> OwningPipeline<I, F, U>
where
    U: UserData,
    F: UniversalRead + 'static,
    I: ReadPipelineImpl<'static, U, File = F>,
{
    pub fn new(file: F) -> Result<Self> {
        Ok(Self {
            impl_: ManuallyDrop::new(I::new()?),
            file: ManuallyDrop::new(file),
            _user_data: PhantomData,
        })
    }

    #[inline]
    pub fn can_schedule(&mut self) -> bool {
        self.impl_.can_schedule()
    }

    pub fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        // SAFETY: `self.file` is owned and, per the `Drop` impl, outlives
        // `self.impl_` (and therefore any in-flight operation that borrows it).
        // Extending the borrow to `'static` lets the impl hold it; the result
        // lifetime is shortened back to `&mut self` in `wait`.
        let file: &'static F = unsafe { &*(&*self.file as *const F) };
        self.impl_.schedule::<P>(user_data, file, range, align)
    }

    /// Like [`schedule`](Self::schedule), but reads the entire file (byte-aligned).
    pub fn schedule_whole(&mut self, user_data: U) -> Result<()> {
        let length = self.file.len::<u8>()?;
        self.schedule::<Sequential>(user_data, 0..length, 1)
    }

    #[inline]
    pub fn wait(&mut self) -> Result<Option<(U, ACow<'_>)>> {
        // Shortens the impl's `ACow<'static>` to `ACow<'_>` (tied to `&mut
        // self`), which conservatively reflects that borrowed data lives inside
        // the owned file held by this pipeline.
        self.impl_.wait()
    }

    #[inline]
    pub fn wait_bytemuck<T: Item>(&mut self) -> Result<Option<(U, Cow<'_, [T]>)>> {
        let Some((user_data, bytes)) = self.wait()? else {
            return Ok(None);
        };
        Ok(Some((user_data, bytes.try_cast_bytemuck().unwrap())))
    }
}

impl<I, F, U> Drop for OwningPipeline<I, F, U> {
    fn drop(&mut self) {
        // Drop `impl_` before `file`.
        // SAFETY: both fields are taken exactly once and not used afterwards.
        unsafe {
            ManuallyDrop::drop(&mut self.impl_);
            ManuallyDrop::drop(&mut self.file);
        }
    }
}
