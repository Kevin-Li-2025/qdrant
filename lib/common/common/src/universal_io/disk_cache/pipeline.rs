use std::ops::Range;

use super::CachedSlice;
use crate::ext::aligned_vec::ACow;
use crate::generic_consts::AccessPattern;
use crate::universal_io::{ReadPipelineImpl, Result, UniversalIoError, UserData};

/// Synchronous block-cache read pipeline. Reads resolve immediately at
/// `schedule` time and borrow from the cached slice (`'file`); the queue holds
/// at most one pending result.
pub struct DiskCacheReadPipeline<'file, U> {
    result: Option<(U, ACow<'file>)>,
}

impl<'file, U> ReadPipelineImpl<'file, U> for DiskCacheReadPipeline<'file, U>
where
    U: UserData,
{
    type File = CachedSlice;

    fn new() -> Result<Self> {
        Ok(Self { result: None })
    }

    fn can_schedule(&mut self) -> bool {
        self.result.is_none()
    }

    fn schedule<P>(
        &mut self,
        user_data: U,
        file: &'file CachedSlice,
        range: Range<u64>,
        align: usize,
    ) -> Result<()>
    where
        P: AccessPattern,
    {
        if self.result.is_some() {
            return Err(UniversalIoError::QueueIsFull);
        }

        let byte_range = range.start as usize..range.end as usize;
        self.result = Some((user_data, file.get_range_bytes(byte_range, align)?));
        Ok(())
    }

    fn wait(&mut self) -> Result<Option<(U, ACow<'file>)>> {
        Ok(self.result.take())
    }
}
