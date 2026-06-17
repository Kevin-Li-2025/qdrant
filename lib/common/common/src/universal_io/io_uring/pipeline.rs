use std::ops::Range;

use ::io_uring::types::Fd;

use super::pool::IO_URING_QUEUE_LENGTH;
use super::{IoUringFile, IoUringRuntime};
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{ReadPipelineImpl, Result, UniversalIoError, UserData};

/// io_uring read pipeline.
///
/// Reads always produce *owned* (`AVec`) buffers, so the `'file` lifetime never
/// bounds returned data — it is purely a file-safety guard ensuring the file's
/// fd outlives all in-flight operations (the runtime drains them on drop). The
/// file-owning variant is provided generically by
/// [`OwningPipeline`](crate::universal_io::OwningPipeline).
pub struct IoUringPipeline<'file, U>
where
    U: UserData,
{
    runtime: IoUringRuntime<'file, U>,
}

impl<'file, U> IoUringPipeline<'file, U>
where
    U: UserData,
{
    /// # Safety
    ///
    /// The caller must ensure that the `fd` will not outlive the pipeline.
    unsafe fn schedule_fd(
        &mut self,
        user_data: U,
        fd: Fd,
        direct_io: bool,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        let mut squeue = self.runtime.io_uring.submission();

        if self.runtime.in_progress + squeue.len() >= IO_URING_QUEUE_LENGTH as _ {
            return Err(UniversalIoError::QueueIsFull);
        }

        let entry = self
            .runtime
            .state
            .read(user_data, fd, range, align, direct_io);

        unsafe {
            squeue.push(&entry).expect("submission queue is not full");
        }

        Ok(())
    }
}

impl<'file, U> ReadPipelineImpl<'file, U> for IoUringPipeline<'file, U>
where
    U: UserData,
{
    type File = IoUringFile;

    fn new() -> Result<Self> {
        Ok(Self {
            runtime: IoUringRuntime::new()?,
        })
    }

    fn can_schedule(&mut self) -> bool {
        let squeue = self.runtime.io_uring.submission();
        self.runtime.in_progress + squeue.len() < IO_URING_QUEUE_LENGTH as _
    }

    fn schedule<P: AccessPattern>(
        &mut self,
        user_data: U,
        file: &'file IoUringFile,
        range: Range<u64>,
        align: usize,
    ) -> Result<()> {
        // SAFETY: `file` outlives the pipeline (`'file`), so `file.fd()` will not
        // outlive any in-flight operation scheduled here.
        unsafe { self.schedule_fd(user_data, file.fd(), file.direct_io, range, align) }
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        let next = self.runtime.completed().next();

        let enqueued = self.runtime.enqueued();

        if next.is_some() && enqueued > 0 {
            self.runtime.submit_and_wait(0)?;
        } else if next.is_none() && enqueued + self.runtime.in_progress > 0 {
            self.runtime.submit_and_wait(1)?;
        }

        let Some(result) = next.or_else(|| self.runtime.completed().next()) else {
            return Ok(None);
        };

        let (user_data, resp) = result?;
        Ok(Some((user_data, ACow::Owned(resp.expect_read()))))
    }
}
