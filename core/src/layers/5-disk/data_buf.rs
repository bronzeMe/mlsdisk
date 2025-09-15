//! Data buffering.
use super::sworndisk::RecordKey;
use crate::layers::bio::{BufMut, BufRef};
use crate::os::{BTreeMap, Condvar, CvarMutex, Mutex};
use crate::prelude::*;

use core::ops::RangeInclusive;

/// A buffer to cache data blocks before they are written to disk.
#[derive(Debug)]
pub(super) struct DataBuf {
    buf: Mutex<BTreeMap<RecordKey, Arc<DataBlock>>>,
    cap: usize,
    cvar: Condvar,
    is_full: CvarMutex<bool>,
}

/// User data block.
pub(super) struct DataBlock([u8; BLOCK_SIZE]);

impl DataBuf {
    /// Create a new empty data buffer with a given capacity.
    pub fn new(cap: usize) -> Self {
        Self {
            buf: Mutex::new(BTreeMap::new()),
            cap,
            cvar: Condvar::new(),
            is_full: CvarMutex::new(false),
        }
    }

    /// Get the buffered data block with the key and copy
    /// the content into `buf`.
    pub fn get(&self, key: RecordKey, buf: &mut BufMut) -> Option<()> {
        debug_assert_eq!(buf.nblocks(), 1);
        if let Some(block) = self.buf.lock().get(&key) {
            buf.as_mut_slice().copy_from_slice(block.as_slice());
            Some(())
        } else {
            None
        }
    }

    /// Get the buffered data blocks which keys are within the given range.
    pub fn get_range(&self, range: RangeInclusive<RecordKey>) -> Vec<(RecordKey, Arc<DataBlock>)> {
        self.buf
            .lock()
            .iter()
            .filter_map(|(k, v)| {
                if range.contains(k) {
                    Some((*k, v.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Put the data block in `buf` into the buffer. Return
    /// whether the buffer is full after insertion.
    pub fn put(&self, key: RecordKey, buf: BufRef) -> bool {
        debug_assert_eq!(buf.nblocks(), 1);

        // CRITICAL FIX: Reorganize locking to prevent deadlock
        loop {
            // ①Check if buffer is full without holding locks for long
            {
                let is_full_guard = self.is_full.lock().unwrap();
                if !*is_full_guard {
                    // Buffer not full, proceed to insert
                    break;
                }
            } // Release is_full lock before waiting
            
            // ②Wait for buffer to become available
            let mut is_full = self.is_full.lock().unwrap();
            while *is_full {
                is_full = self.cvar.wait(is_full).unwrap();
            }
            // Loop back to check again
        }

        // ③Now safely acquire both locks in consistent order
        let mut data_buf = self.buf.lock();
        let mut is_full = self.is_full.lock().unwrap();
        
        // ④Double-check capacity after acquiring locks
        if data_buf.len() >= self.cap {
            *is_full = true;
            return true;
        }
        
        // ⑤Insert data block
        let _ = data_buf.insert(key, DataBlock::from_buf(buf));
        let buffer_full = data_buf.len() >= self.cap;
        
        // ⑥Update full status
        if buffer_full {
            *is_full = true;
        }
        
        buffer_full
    }

    /// Return the number of data blocks of the buffer.
    pub fn nblocks(&self) -> usize {
        self.buf.lock().len()
    }

    /// Return whether the buffer is full.
    pub fn at_capacity(&self) -> bool {
        self.nblocks() >= self.cap
    }

    /// Return whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.nblocks() == 0
    }

    /// Empty the buffer.
    pub fn clear(&self) {
        // CRITICAL FIX: Use consistent lock order to prevent deadlock
        let mut data_buf = self.buf.lock();              // ①First acquire buf lock
        let mut is_full = self.is_full.lock().unwrap();  // ②Then acquire is_full lock
        
        data_buf.clear();                                // ③Clear the buffer
        
        if *is_full {
            *is_full = false;                            // ④Update full status
            self.cvar.notify_all();                      // ⑤Notify waiting threads
        }
    }

    /// Return all the buffered data blocks.
    pub fn all_blocks(&self) -> Vec<(RecordKey, Arc<DataBlock>)> {
        self.buf
            .lock()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }
}

impl DataBlock {
    /// Create a new data block from the given `buf`.
    pub fn from_buf(buf: BufRef) -> Arc<Self> {
        debug_assert_eq!(buf.nblocks(), 1);
        Arc::new(DataBlock(buf.as_slice().try_into().unwrap()))
    }

    /// Return the immutable slice of the data block.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataBlock")
            .field("first 16 bytes", &&self.0[..16])
            .finish()
    }
}
