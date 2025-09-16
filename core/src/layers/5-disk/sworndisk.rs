//! SwornDisk as a block device.
//!
//! API: submit_bio(), submit_bio_sync(), create(), open(),
//! read(), readv(), write(), writev(), sync().
//!
//! Responsible for managing a `TxLsmTree`, whereas the TX logs (WAL and SSTs)
//! are stored; an untrusted disk storing user data, a `BlockAlloc` for managing data blocks'
//! allocation metadata. `TxLsmTree` and `BlockAlloc` are manipulated
//! based on internal transactions.
use super::bio::{BioReq, BioReqQueue, BioResp, BioType};
use super::block_alloc::{AllocTable, BlockAlloc};
use super::data_buf::DataBuf;
use super::read_cache::{ReadCacheSystem, CacheLookupResult, CacheInsertHint};
use crate::layers::bio::{BlockId, BlockSet, Buf, BufMut, BufRef};
use crate::layers::log::TxLogStore;
use crate::layers::lsm::{
    AsKV, LsmLevel, RangeQueryCtx, RecordKey as RecordK, RecordValue as RecordV, SyncIdStore,
    TxEventListener, TxEventListenerFactory, TxLsmTree, TxType,
};
use crate::os::{Aead, AeadIv as Iv, AeadKey as Key, AeadMac as Mac, RwLock};
use crate::prelude::*;
use crate::tx::Tx;

use core::num::NonZeroUsize;
use core::ops::{Add, Sub};
use core::sync::atomic::{AtomicBool, Ordering};
use pod::Pod;

/// Logical Block Address.
pub type Lba = BlockId;
/// Host Block Address.
pub type Hba = BlockId;

/// SwornDisk.
pub struct SwornDisk<D: BlockSet> {
    inner: Arc<DiskInner<D>>,
}

/// Inner structures of `SwornDisk`.
struct DiskInner<D: BlockSet> {
    /// Block I/O request queue.
    bio_req_queue: BioReqQueue,
    /// A `TxLsmTree` to store metadata of the logical blocks.
    logical_block_table: TxLsmTree<RecordKey, RecordValue, D>,
    /// The underlying disk where user data is stored.
    user_data_disk: D,
    /// Manage space of the data disk.
    block_validity_table: Arc<AllocTable>,
    /// TX log store for managing logs in `TxLsmTree` and block alloc logs.
    tx_log_store: Arc<TxLogStore<D>>,
    /// A buffer to cache data blocks.
    data_buf: DataBuf,
    /// Optional three-tier intelligent read cache system.
    read_cache: Option<Arc<ReadCacheSystem>>,
    /// Root encryption key.
    root_key: Key,
    /// Whether `SwornDisk` is dropped.
    is_dropped: AtomicBool,
    /// Scope lock for control write and sync operation.
    write_sync_region: RwLock<()>,
}

impl<D: BlockSet + 'static> SwornDisk<D> {
    /// Read a specified number of blocks at a logical block address on the device.
    /// The block contents will be read into a single contiguous buffer.
    pub fn read(&self, lba: Lba, buf: BufMut) -> Result<()> {
        self.check_rw_args(lba, buf.nblocks())?;
        self.inner.read(lba, buf)
    }

    /// Read multiple blocks at a logical block address on the device.
    /// The block contents will be read into several scattered buffers.
    pub fn readv<'a>(&self, lba: Lba, bufs: &'a mut [BufMut<'a>]) -> Result<()> {
        self.check_rw_args(lba, bufs.iter().fold(0, |acc, buf| acc + buf.nblocks()))?;
        self.inner.readv(lba, bufs)
    }

    /// Write a specified number of blocks at a logical block address on the device.
    /// The block contents reside in a single contiguous buffer.
    pub fn write(&self, lba: Lba, buf: BufRef) -> Result<()> {
        self.check_rw_args(lba, buf.nblocks())?;
        let _rguard = self.inner.write_sync_region.read();
        self.inner.write(lba, buf)
    }

    /// Write multiple blocks at a logical block address on the device.
    /// The block contents reside in several scattered buffers.
    pub fn writev(&self, lba: Lba, bufs: &[BufRef]) -> Result<()> {
        self.check_rw_args(lba, bufs.iter().fold(0, |acc, buf| acc + buf.nblocks()))?;
        let _rguard = self.inner.write_sync_region.read();
        self.inner.writev(lba, bufs)
    }

    /// Sync all cached data in the device to the storage medium for durability.
    pub fn sync(&self) -> Result<()> {
        let _wguard = self.inner.write_sync_region.write();
        // TODO: Error handling the sync operation
        self.inner.sync().unwrap();

        #[cfg(not(feature = "linux"))]
        trace!("[SwornDisk] Sync completed. {self:?}");
        Ok(())
    }

    /// Returns the total number of blocks in the device.
    pub fn total_blocks(&self) -> usize {
        self.inner.user_data_disk.nblocks()
    }

    /// Get read cache statistics for monitoring and optimization.
    ///
    /// Returns cache hit ratios, memory usage, and other performance metrics
    /// that can be used to tune cache behavior and monitor system performance.
    /// If read cache is disabled, returns default (empty) statistics.
    pub fn cache_stats(&self) -> super::read_cache::CacheStats {
        if let Some(ref read_cache) = self.inner.read_cache {
            read_cache.stats()
        } else {
            // Return default empty stats when cache is disabled
            super::read_cache::CacheStats::default()
        }
    }

    /// Creates a new `SwornDisk` on the given disk, with the root encryption key.
    pub fn create(
        disk: D,
        root_key: Key,
        sync_id_store: Option<Arc<dyn SyncIdStore>>,
        enable_read_cache: bool,
    ) -> Result<Self> {
        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Starting create process with {} blocks", disk.nblocks());
        
        let data_disk = Self::subdisk_for_data(&disk)?;
        let lsm_tree_disk = Self::subdisk_for_logical_block_table(&disk)?;
        
        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Partitioned disk: data={} blocks, lsm_tree={} blocks", 
               data_disk.nblocks(), lsm_tree_disk.nblocks());

        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Formatting TxLogStore...");
        let tx_log_store = Arc::new(TxLogStore::format(lsm_tree_disk, root_key.clone())?);
        
        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Creating AllocTable for {} blocks...", data_disk.nblocks());
        let block_validity_table = Arc::new(AllocTable::new(
            NonZeroUsize::new(data_disk.nblocks()).unwrap(),
        ));
        
        let read_cache = if enable_read_cache {
            #[cfg(not(feature = "linux"))]
            debug!("[SwornDisk] Initializing ReadCacheSystem...");
            Some(Arc::new(ReadCacheSystem::new()?))
        } else {
            #[cfg(not(feature = "linux"))]
            debug!("[SwornDisk] ReadCacheSystem disabled");
            None
        };
        
        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Creating TxLsmTreeListenerFactory...");
        let listener_factory = Arc::new(TxLsmTreeListenerFactory::new(
            tx_log_store.clone(),
            block_validity_table.clone(),
            read_cache.clone(),
        ));

        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Formatting TxLsmTree...");
        let logical_block_table = {
            let table = block_validity_table.clone();
            let on_drop_record_in_memtable = move |record: &dyn AsKV<RecordKey, RecordValue>| {
                // Deallocate the host block while the corresponding record is dropped in `MemTable`
                table.set_deallocated(record.value().hba);
            };
            TxLsmTree::format(
                tx_log_store.clone(),
                listener_factory,
                Some(Arc::new(on_drop_record_in_memtable)),
                sync_id_store,
            )?
        };

        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Assembling SwornDisk components...");
        let new_self = Self {
            inner: Arc::new(DiskInner {
                bio_req_queue: BioReqQueue::new(),
                logical_block_table,
                user_data_disk: data_disk,
                block_validity_table,
                tx_log_store,
                data_buf: DataBuf::new(DATA_BUF_CAP),
                read_cache,
                root_key,
                is_dropped: AtomicBool::new(false),
                write_sync_region: RwLock::new(()),
            }),
        };

        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Create completed successfully");
        // XXX: Would `disk::drop()` bring unexpected behavior?
        Ok(new_self)
    }

    /// Opens the `SwornDisk` on the given disk, with the root encryption key.
    pub fn open(
        disk: D,
        root_key: Key,
        sync_id_store: Option<Arc<dyn SyncIdStore>>,
        enable_read_cache: bool,
    ) -> Result<Self> {
        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Starting open process with {} blocks", disk.nblocks());
        
        let data_disk = Self::subdisk_for_data(&disk)?;
        let lsm_tree_disk = Self::subdisk_for_logical_block_table(&disk)?;

        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Recovering TxLogStore...");
        let tx_log_store = Arc::new(TxLogStore::recover(lsm_tree_disk, root_key)?);
        
        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Recovering AllocTable...");
        let block_validity_table = Arc::new(AllocTable::recover(
            NonZeroUsize::new(data_disk.nblocks()).unwrap(),
            &tx_log_store,
        )?);
        
        let read_cache = if enable_read_cache {
            #[cfg(not(feature = "linux"))]
            debug!("[SwornDisk] Initializing ReadCacheSystem...");
            Some(Arc::new(ReadCacheSystem::new()?))
        } else {
            #[cfg(not(feature = "linux"))]
            debug!("[SwornDisk] ReadCacheSystem disabled");
            None
        };
        
        let listener_factory = Arc::new(TxLsmTreeListenerFactory::new(
            tx_log_store.clone(),
            block_validity_table.clone(),
            read_cache.clone(),
        ));

        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Recovering TxLsmTree...");
        let logical_block_table = {
            let table = block_validity_table.clone();
            let on_drop_record_in_memtable = move |record: &dyn AsKV<RecordKey, RecordValue>| {
                // Deallocate the host block while the corresponding record is dropped in `MemTable`
                table.set_deallocated(record.value().hba);
            };
            TxLsmTree::recover(
                tx_log_store.clone(),
                listener_factory,
                Some(Arc::new(on_drop_record_in_memtable)),
                sync_id_store,
            )?
        };

        let opened_self = Self {
            inner: Arc::new(DiskInner {
                bio_req_queue: BioReqQueue::new(),
                logical_block_table,
                user_data_disk: data_disk,
                block_validity_table,
                data_buf: DataBuf::new(DATA_BUF_CAP),
                read_cache,
                tx_log_store,
                root_key,
                is_dropped: AtomicBool::new(false),
                write_sync_region: RwLock::new(()),
            }),
        };

        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Open completed successfully");
        Ok(opened_self)
    }

    /// Submit a new block I/O request and wait its completion (Synchronous).
    pub fn submit_bio_sync(&self, bio_req: BioReq) -> BioResp {
        bio_req.submit();
        self.inner.handle_bio_req(&bio_req)
    }
    // TODO: Support handling request asynchronously

    /// Check whether the arguments are valid for read/write operations.
    fn check_rw_args(&self, lba: Lba, buf_nblocks: usize) -> Result<()> {
        if lba + buf_nblocks > self.inner.user_data_disk.nblocks() {
            Err(Error::with_msg(
                OutOfDisk,
                "read/write out of disk capacity",
            ))
        } else {
            Ok(())
        }
    }

    fn subdisk_for_data(disk: &D) -> Result<D> {
        disk.subset(0..disk.nblocks() * 15 / 16) // TBD
    }

    fn subdisk_for_logical_block_table(disk: &D) -> Result<D> {
        disk.subset(disk.nblocks() * 15 / 16..disk.nblocks()) // TBD
    }
}

/// Capacity of the user data blocks buffer.
/// CRITICAL FIX: Increased from 1024 to 8192 blocks (32MB) to handle large filesystem 
/// formatting operations like mke2fs which can write significant amounts of metadata.
/// This prevents data loss during frequent DataBuf flushes that could cause 
/// inconsistencies in the LSM tree records.
const DATA_BUF_CAP: usize = 8192;

impl<D: BlockSet + 'static> DiskInner<D> {
    /// Read a specified number of blocks at a logical block address on the device.
    /// The block contents will be read into a single contiguous buffer.
    pub fn read(&self, lba: Lba, buf: BufMut) -> Result<()> {
        let nblocks = buf.nblocks();

        let res = if nblocks == 1 {
            self.read_one_block(lba, buf)
        } else {
            self.read_multi_blocks(lba, &mut [buf])
        };

        // Allow empty read
        if let Err(e) = &res
            && e.errno() == NotFound
        {
            // Silently handle empty reads to avoid SGX logging issues
            // #[cfg(not(feature = "linux"))]
            // warn!("[SwornDisk] read contains empty read on lba {lba}");
            return Ok(());
        }
        res
    }

    /// Read multiple blocks at a logical block address on the device.
    /// The block contents will be read into several scattered buffers.
    pub fn readv<'a>(&self, lba: Lba, bufs: &'a mut [BufMut<'a>]) -> Result<()> {
        let res = self.read_multi_blocks(lba, bufs);

        // Allow empty read
        if let Err(e) = &res
            && e.errno() == NotFound
        {
            // Silently handle empty reads to avoid SGX logging issues
            // #[cfg(not(feature = "linux"))]
            // warn!("[SwornDisk] readv contains empty read on lba {lba}");
            return Ok(());
        }
        res
    }

    fn read_one_block(&self, lba: Lba, mut buf: BufMut) -> Result<()> {
        debug_assert_eq!(buf.nblocks(), 1);
        let key = RecordKey { lba };
        
        // Search in write buffer (`DataBuf`) first - highest priority
        if self.data_buf.get(key, &mut buf).is_some() {
            return Ok(());
        }

        // Check read cache system next (if enabled)
        if let Some(ref read_cache) = self.read_cache {
            match read_cache.lookup_and_copy(key, &mut buf) {
                CacheLookupResult::Hit => {
                    // Single-copy optimization: data directly copied to buffer
                    return Ok(());
                }
                CacheLookupResult::Miss => {
                    // Continue to disk read
                }
            }
        }

        // Fallback to LSM tree lookup and disk read
        let value = self.logical_block_table.get(&key)?;

        // Perform disk read and decryption
        let mut cipher = Buf::alloc(1)?;
        self.user_data_disk.read(value.hba, cipher.as_mut())?;
        Aead::new().decrypt(
            cipher.as_slice(),
            &value.key,
            &Iv::new_zeroed(),
            &[],
            &value.mac,
            buf.as_mut_slice(),
        )?;

        // SMART CACHING STRATEGY: Use adaptive caching for single block reads
        // Cache single blocks selectively based on access patterns to balance performance
        if let Some(ref read_cache) = self.read_cache {
            // Heuristic: Cache blocks that are likely to be accessed again
            // - Metadata blocks (low LBA ranges are often metadata)
            // - Recently accessed blocks in the same vicinity
            // - Skip caching for obvious sequential patterns
            
            let should_cache = self.should_cache_single_block(lba);
            
            if should_cache {
                let cached_data: Box<[u8; BLOCK_SIZE]> = Box::new(
                    buf.as_slice().try_into().map_err(|_| {
                        Error::with_msg(InvalidArgs, "buffer size mismatch")
                    })?
                );
                // Use Cold hint for single blocks to prioritize multi-block cache entries
                if let Err(_) = read_cache.insert(key, cached_data, CacheInsertHint::Cold) {
                    // Silently handle cache insertion failures
                }
            }
        }

        Ok(())
    }

    /// Smart heuristic to decide whether to cache a single block read.
    /// Balances cache utilization with performance overhead.
    fn should_cache_single_block(&self, lba: Lba) -> bool {
        // Heuristic 1: Always cache low LBA ranges (likely filesystem metadata)
        // EXT2 metadata (superblock, group descriptors, inode tables) are typically
        // in the first few thousand blocks and are frequently accessed
        if lba < 8192 {  // First 32MB likely contains metadata
            return true;
        }
        
        // Heuristic 2: Cache with probability based on LBA to avoid cache pollution
        // Higher LBAs (user data) get cached less frequently
        // This creates a natural preference for metadata over user data
        let cache_probability = if lba < 65536 {  // First 256MB
            0.5  // 50% chance for early data blocks
        } else if lba < 262144 {  // First 1GB  
            0.2  // 20% chance for mid-range blocks
        } else {
            0.1  // 10% chance for high-range blocks (likely sequential data)
        };
        
        // Simple pseudo-random decision based on LBA
        // This ensures deterministic behavior for the same LBA
        (lba as u32).wrapping_mul(2654435761) % 100 < (cache_probability * 100.0) as u32
    }

    fn read_multi_blocks<'a>(&self, lba: Lba, bufs: &'a mut [BufMut<'a>]) -> Result<()> {
        let mut buf_vec = BufMutVec::from_bufs(bufs);
        let nblocks = buf_vec.nblocks();

        let mut range_query_ctx =
            RangeQueryCtx::<RecordKey, RecordValue>::new(RecordKey { lba }, nblocks);

        // Search in `DataBuf` first
        for (key, data_block) in self
            .data_buf
            .get_range(range_query_ctx.range_uncompleted().unwrap())
        {
            buf_vec
                .nth_buf_mut_slice(key.lba - lba)
                .copy_from_slice(data_block.as_slice());
            range_query_ctx.mark_completed(key);
        }
        if range_query_ctx.is_completed() {
            return Ok(());
        }

        // PERFORMANCE OPTIMIZATION: Detect sequential read pattern to skip cache operations
        // Sequential reads (large continuous blocks) have very low cache hit probability
        // but high cache operation overhead. Skip cache for large sequential reads.
        let is_likely_sequential = nblocks >= 32; // 128KB+ reads are likely sequential
        let enable_cache_operations = !is_likely_sequential && self.read_cache.is_some();

        // Check read cache for remaining uncompleted blocks (only if beneficial)
        if enable_cache_operations {
            let read_cache = self.read_cache.as_ref().unwrap();
            if let Some(uncompleted_range) = range_query_ctx.range_uncompleted() {
                for current_lba in uncompleted_range.start().lba..=uncompleted_range.end().lba {
                    let key = RecordKey { lba: current_lba };
                    
                    // Only check uncompleted blocks
                    if !range_query_ctx.contains_uncompleted(&key) {
                        continue;
                    }
                    
                    // Check read cache for this specific block  
                    let target_slice = buf_vec.nth_buf_mut_slice(current_lba - lba);
                    match read_cache.lookup_and_copy_to_slice(key, target_slice) {
                        CacheLookupResult::Hit => {
                            // Single-copy optimization: data directly copied to target slice
                            range_query_ctx.mark_completed(key);
                        }
                        CacheLookupResult::Miss => {
                            // Cache miss - will be handled by LSM Tree lookup
                        }
                    }
                }
            }
        }
        
        if range_query_ctx.is_completed() {
            return Ok(());
        }

        // Search in `TxLsmTree` then
        self.logical_block_table.get_range(&mut range_query_ctx)?;
        // Allow empty read
        debug_assert!(range_query_ctx.is_completed());

        let mut res = range_query_ctx.into_results();
        let record_batches = {
            res.sort_by(|(_, v1), (_, v2)| v1.hba.cmp(&v2.hba));
            res.group_by(|(_, v1), (_, v2)| v2.hba - v1.hba == 1)
        };

        // Perform disk read in batches and decryption
        let mut cipher_buf = Buf::alloc(nblocks)?;
        let cipher_slice = cipher_buf.as_mut_slice();
        for record_batch in record_batches {
            self.user_data_disk.read(
                record_batch.first().unwrap().1.hba,
                BufMut::try_from(&mut cipher_slice[..record_batch.len() * BLOCK_SIZE]).unwrap(),
            )?;

            for (nth, (key, value)) in record_batch.iter().enumerate() {
                let buf_slice = buf_vec.nth_buf_mut_slice(key.lba - lba);
                Aead::new().decrypt(
                    &cipher_slice[nth * BLOCK_SIZE..(nth + 1) * BLOCK_SIZE],
                    &value.key,
                    &Iv::new_zeroed(),
                    &[],
                    &value.mac,
                    buf_slice,
                )?;
                
                // PERFORMANCE OPTIMIZATION: Only cache if likely to be beneficial
                // Skip cache insertion for sequential reads to avoid allocation overhead
                if enable_cache_operations {
                    let read_cache = self.read_cache.as_ref().unwrap();
                    let cached_data: Box<[u8; BLOCK_SIZE]> = Box::new(
                        buf_slice.try_into().map_err(|_| {
                            Error::with_msg(InvalidArgs, "buffer size mismatch")
                        })?
                    );
                    if let Err(_) = read_cache.insert(*key, cached_data, CacheInsertHint::Normal) {
                        // Silently handle cache insertion failures to avoid SGX logging issues
                        // #[cfg(not(feature = "linux"))]
                        // warn!("[SwornDisk] Failed to insert block {} into read cache", key.lba);
                    }
                }
            }
        }

        Ok(())
    }

    /// Write a specified number of blocks at a logical block address on the device.
    /// The block contents reside in a single contiguous buffer.
    pub fn write(&self, mut lba: Lba, buf: BufRef) -> Result<()> {
        // CRITICAL FIX: Invalidate read cache immediately for written blocks
        let mut written_keys = Vec::with_capacity(buf.nblocks());
        
        // Write block contents to `DataBuf` directly
        for block_buf in buf.iter() {
            let key = RecordKey { lba };
            written_keys.push(key);
            
            let buf_at_capacity = self.data_buf.put(key, block_buf);

            // Flush all data blocks in `DataBuf` to disk if it's full
            if buf_at_capacity {
                // TODO: Error handling: Should discard current write in `DataBuf`
                self.flush_data_buf()?;
            }
            lba += 1;
        }
        
        // CRITICAL FIX: Immediately invalidate read cache for written blocks (if cache is enabled)
        // This prevents stale cache data from being returned before flush
        if !written_keys.is_empty() {
            if let Some(ref read_cache) = self.read_cache {
                let _invalidated = read_cache.invalidate_batch(&written_keys);
                // Silent invalidation for better performance
            }
        }
        
        Ok(())
    }

    /// Write multiple blocks at a logical block address on the device.
    /// The block contents reside in several scattered buffers.
    pub fn writev(&self, mut lba: Lba, bufs: &[BufRef]) -> Result<()> {
        // CRITICAL FIX: Collect all keys for immediate cache invalidation
        let total_blocks: usize = bufs.iter().map(|buf| buf.nblocks()).sum();
        let mut all_written_keys = Vec::with_capacity(total_blocks);
        
        for buf in bufs {
            // CRITICAL FIX: Collect keys before calling write() which will also invalidate
            for i in 0..buf.nblocks() {
                all_written_keys.push(RecordKey { lba: lba + i });
            }
            self.write(lba, *buf)?;
            lba += buf.nblocks();
        }
        
        // Note: Individual write() calls already invalidated their respective blocks
        // This is just for consistency and potential future optimizations
        Ok(())
    }

    fn flush_data_buf(&self) -> Result<()> {
        let records = self.write_blocks_from_data_buf()?;
        
        // Insert new records of data blocks to `TxLsmTree`
        for (key, value) in records {
            // TODO: Error handling: Should dealloc the written blocks
            self.logical_block_table.put(key, value)?;
        }

        self.data_buf.clear();
        
        // NOTE: read_cache invalidation is now handled immediately in write()
        // operations to maintain better data consistency, so no need to 
        // invalidate again here. This avoids duplicate invalidation overhead.
        
        Ok(())
    }

    fn write_blocks_from_data_buf(&self) -> Result<Vec<(RecordKey, RecordValue)>> {
        let data_blocks = self.data_buf.all_blocks();

        let num_write = data_blocks.len();
        let mut records = Vec::with_capacity(num_write);
        if num_write == 0 {
            return Ok(records);
        }

        // Allocate slots for data blocks
        let hbas = self
            .block_validity_table
            .alloc_batch(NonZeroUsize::new(num_write).unwrap())?;
        debug_assert_eq!(hbas.len(), num_write);
        let hba_batches = hbas.group_by(|hba1, hba2| hba2 - hba1 == 1);

        // Perform encryption and batch disk write
        let mut cipher_buf = Buf::alloc(num_write)?;
        let mut cipher_slice = cipher_buf.as_mut_slice();
        let mut nth = 0;
        for hba_batch in hba_batches {
            for (i, &hba) in hba_batch.iter().enumerate() {
                let (lba, data_block) = &data_blocks[nth];
                let key = Key::random();
                let mac = Aead::new().encrypt(
                    data_block.as_slice(),
                    &key,
                    &Iv::new_zeroed(),
                    &[],
                    &mut cipher_slice[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE],
                )?;

                records.push((*lba, RecordValue { hba, key, mac }));
                nth += 1;
            }

            self.user_data_disk.write(
                *hba_batch.first().unwrap(),
                BufRef::try_from(&cipher_slice[..hba_batch.len() * BLOCK_SIZE]).unwrap(),
            )?;
            cipher_slice = &mut cipher_slice[hba_batch.len() * BLOCK_SIZE..];
        }

        Ok(records)
    }

    /// Sync all cached data in the device to the storage medium for durability.
    pub fn sync(&self) -> Result<()> {
        self.flush_data_buf()?;
        debug_assert!(self.data_buf.is_empty());

        self.logical_block_table.sync()?;

        // XXX: May impact performance when there comes frequent syncs
        self.block_validity_table
            .do_compaction(&self.tx_log_store)?;

        self.tx_log_store.sync()?;

        self.user_data_disk.flush()
    }

    /// Handle one block I/O request. Mark the request completed when finished,
    /// return any error that occurs.
    pub fn handle_bio_req(&self, req: &BioReq) -> BioResp {
        let res = match req.type_() {
            BioType::Read => self.do_read(&req),
            BioType::Write => self.do_write(&req),
            BioType::Sync => self.do_sync(&req),
        };

        req.complete(res.clone());
        res
    }

    /// Handle a read I/O request.
    fn do_read(&self, req: &BioReq) -> BioResp {
        debug_assert_eq!(req.type_(), BioType::Read);

        let lba = req.addr() as Lba;
        let mut req_bufs = req.take_bufs();
        let mut bufs = {
            let mut bufs = Vec::with_capacity(req.nbufs());
            for buf in req_bufs.iter_mut() {
                bufs.push(BufMut::try_from(buf.as_mut_slice())?);
            }
            bufs
        };

        if bufs.len() == 1 {
            let buf = bufs.remove(0);
            return self.read(lba, buf);
        }

        self.readv(lba, &mut bufs)
    }

    /// Handle a write I/O request.
    fn do_write(&self, req: &BioReq) -> BioResp {
        debug_assert_eq!(req.type_(), BioType::Write);

        let lba = req.addr() as Lba;
        let req_bufs = req.take_bufs();
        let bufs = {
            let mut bufs = Vec::with_capacity(req.nbufs());
            for buf in req_bufs.iter() {
                bufs.push(BufRef::try_from(buf.as_slice())?);
            }
            bufs
        };

        self.writev(lba, &bufs)
    }

    /// Handle a sync I/O request.
    fn do_sync(&self, req: &BioReq) -> BioResp {
        debug_assert_eq!(req.type_(), BioType::Sync);
        self.sync()
    }
}

impl<D: BlockSet> Drop for SwornDisk<D> {
    fn drop(&mut self) {
        self.inner.is_dropped.store(true, Ordering::Release);
    }
}

impl<D: BlockSet + 'static> Debug for SwornDisk<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SwornDisk")
            .field("user_data_nblocks", &self.inner.user_data_disk.nblocks())
            .field("logical_block_table", &self.inner.logical_block_table)
            .finish()
    }
}

/// A wrapper for `[BufMut]` used in `readv()`.
struct BufMutVec<'a> {
    bufs: &'a mut [BufMut<'a>],
    nblocks: usize,
}

impl<'a> BufMutVec<'a> {
    pub fn from_bufs(bufs: &'a mut [BufMut<'a>]) -> Self {
        debug_assert!(bufs.len() > 0);
        let nblocks = bufs
            .iter()
            .map(|buf| buf.nblocks())
            .fold(0_usize, |sum, nblocks| sum.saturating_add(nblocks));
        Self { bufs, nblocks }
    }

    pub fn nblocks(&self) -> usize {
        self.nblocks
    }

    pub fn nth_buf_mut_slice(&mut self, mut nth: usize) -> &mut [u8] {
        debug_assert!(nth < self.nblocks);
        for buf in self.bufs.iter_mut() {
            let nblocks = buf.nblocks();
            if nth >= buf.nblocks() {
                nth -= nblocks;
            } else {
                return &mut buf.as_mut_slice()[nth * BLOCK_SIZE..(nth + 1) * BLOCK_SIZE];
            }
        }
        &mut []
    }
}

// SAFETY: `SwornDisk` is concurrency-safe.
unsafe impl<D: BlockSet> Send for DiskInner<D> {}
unsafe impl<D: BlockSet> Sync for DiskInner<D> {}

/// Listener factory for `TxLsmTree`.
struct TxLsmTreeListenerFactory<D> {
    store: Arc<TxLogStore<D>>,
    alloc_table: Arc<AllocTable>,
    /// Optional read cache reference for cache invalidation during compaction
    read_cache: Option<Arc<ReadCacheSystem>>,
}

impl<D> TxLsmTreeListenerFactory<D> {
    fn new(store: Arc<TxLogStore<D>>, alloc_table: Arc<AllocTable>, read_cache: Option<Arc<ReadCacheSystem>>) -> Self {
        Self { store, alloc_table, read_cache }
    }
}

impl<D: BlockSet + 'static> TxEventListenerFactory<RecordKey, RecordValue>
    for TxLsmTreeListenerFactory<D>
{
    fn new_event_listener(
        &self,
        tx_type: TxType,
    ) -> Arc<dyn TxEventListener<RecordKey, RecordValue>> {
        Arc::new(TxLsmTreeListener::new(
            tx_type,
            Arc::new(BlockAlloc::new(
                self.alloc_table.clone(),
                self.store.clone(),
            )),
            self.read_cache.clone(),
        ))
    }
}

/// Event listener for `TxLsmTree`.
struct TxLsmTreeListener<D> {
    tx_type: TxType,
    block_alloc: Arc<BlockAlloc<D>>,
    /// Optional read cache reference for cache invalidation during compaction
    read_cache: Option<Arc<ReadCacheSystem>>,
}

impl<D> TxLsmTreeListener<D> {
    fn new(tx_type: TxType, block_alloc: Arc<BlockAlloc<D>>, read_cache: Option<Arc<ReadCacheSystem>>) -> Self {
        Self {
            tx_type,
            block_alloc,
            read_cache,
        }
    }
}

/// Register callbacks for different TXs in `TxLsmTree`.
impl<D: BlockSet + 'static> TxEventListener<RecordKey, RecordValue> for TxLsmTreeListener<D> {
    fn on_add_record(&self, record: &dyn AsKV<RecordKey, RecordValue>) -> Result<()> {
        match self.tx_type {
            TxType::Compaction { to_level } if to_level == LsmLevel::L0 => {
                self.block_alloc.alloc_block(record.value().hba)
            }
            // Major Compaction TX and Migration TX: invalidate cache for compacted records
            TxType::Compaction { .. } | TxType::Migration => {
                // CRITICAL FIX: Invalidate read cache when records are reorganized during compaction
                // This prevents reading stale cached data that points to old physical locations
                if let Some(ref read_cache) = self.read_cache {
                    read_cache.invalidate(*record.key());
                }
                Ok(())
            }
        }
    }

    fn on_drop_record(&self, record: &dyn AsKV<RecordKey, RecordValue>) -> Result<()> {
        match self.tx_type {
            // Minor Compaction TX doesn't compact records
            TxType::Compaction { to_level } if to_level == LsmLevel::L0 => {
                unreachable!();
            }
            TxType::Compaction { .. } | TxType::Migration => {
                // CRITICAL FIX: Invalidate read cache when old records are dropped during compaction
                // This ensures that any cached data pointing to the old physical location is removed
                if let Some(ref read_cache) = self.read_cache {
                    read_cache.invalidate(*record.key());
                }
                self.block_alloc.dealloc_block(record.value().hba)
            }
        }
    }

    fn on_tx_begin(&self, tx: &mut Tx) -> Result<()> {
        match self.tx_type {
            TxType::Compaction { .. } | TxType::Migration => {
                tx.context(|| self.block_alloc.prepare_diff_log().unwrap())
            }
        }
        Ok(())
    }

    fn on_tx_precommit(&self, tx: &mut Tx) -> Result<()> {
        match self.tx_type {
            TxType::Compaction { .. } | TxType::Migration => {
                tx.context(|| self.block_alloc.update_diff_log().unwrap())
            }
        }
        Ok(())
    }

    fn on_tx_commit(&self) {
        match self.tx_type {
            TxType::Compaction { .. } | TxType::Migration => self.block_alloc.update_alloc_table(),
        }
    }
}

/// Key-Value record for `TxLsmTree`.
pub(super) struct Record {
    key: RecordKey,
    value: RecordValue,
}

/// The key of a `Record`.
#[repr(C)]
#[derive(Clone, Copy, Pod, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(super) struct RecordKey {
    /// Logical block address of user data block.
    pub lba: Lba,
}

/// The value of a `Record`.
#[repr(C)]
#[derive(Clone, Copy, Pod, Debug)]
pub(super) struct RecordValue {
    /// Host block address of user data block.
    pub hba: Hba,
    /// Encryption key of the data block.
    pub key: Key,
    /// Encrypted MAC of the data block.
    pub mac: Mac,
}

impl Add<usize> for RecordKey {
    type Output = Self;

    fn add(self, other: usize) -> Self::Output {
        Self {
            lba: self.lba + other,
        }
    }
}

impl Sub<RecordKey> for RecordKey {
    type Output = usize;

    fn sub(self, other: RecordKey) -> Self::Output {
        self.lba - other.lba
    }
}

impl RecordK<RecordKey> for RecordKey {}
impl RecordV for RecordValue {}

impl AsKV<RecordKey, RecordValue> for Record {
    fn key(&self) -> &RecordKey {
        &self.key
    }

    fn value(&self) -> &RecordValue {
        &self.value
    }
}

#[cfg(feature = "occlum")]
mod impl_block_device {
    use super::{BlockSet, BufMut, BufRef, SwornDisk, Vec};
    use ext2_rs::{Bid, BlockDevice, FsError as Ext2Error};

    impl<D: BlockSet + 'static> BlockDevice for SwornDisk<D> {
        fn total_blocks(&self) -> usize {
            self.total_blocks()
        }

        fn read_blocks(&self, bid: Bid, blocks: &mut [&mut [u8]]) -> Result<(), Ext2Error> {
            if blocks.len() == 1 {
                self.read(
                    bid as _,
                    BufMut::try_from(blocks.first_mut().unwrap().as_mut()).unwrap(),
                )?;
                return Ok(());
            }

            let mut bufs = blocks
                .iter_mut()
                .map(|block| BufMut::try_from(block.as_mut()).unwrap())
                .collect::<Vec<_>>();
            self.readv(bid as _, &mut bufs)?;
            Ok(())
        }

        fn write_blocks(&self, bid: Bid, blocks: &[&[u8]]) -> Result<(), Ext2Error> {
            if blocks.len() == 1 {
                self.write(
                    bid as _,
                    BufRef::try_from(blocks.first().unwrap().as_ref()).unwrap(),
                )?;
                return Ok(());
            }

            let bufs = blocks
                .iter()
                .map(|block| BufRef::try_from(block.as_ref()).unwrap())
                .collect::<Vec<_>>();
            self.writev(bid as _, &bufs)?;
            Ok(())
        }

        fn sync(&self) -> Result<(), Ext2Error> {
            self.sync()?;
            Ok(())
        }
    }

    impl From<crate::Error> for Ext2Error {
        fn from(value: crate::Error) -> Self {
            match value.errno() {
                crate::Errno::NotFound => Self::EntryNotFound,
                crate::Errno::InvalidArgs => Self::InvalidParam,
                crate::Errno::OutOfDisk => Self::NoDeviceSpace,
                crate::Errno::PermissionDenied => Self::PermError,
                _ => {
                    log::error!("[SwornDisk] Error occurred: {value:?}");
                    Self::DeviceError(0)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::bio::MemDisk;
    use crate::layers::disk::bio::{BioReqBuilder, BlockBuf};

    use core::ptr::NonNull;
    use std::thread;

    #[test]
    fn sworndisk_fns() -> Result<()> {
        let nblocks = 64 * 1024;
        let mem_disk = MemDisk::create(nblocks)?;
        let root_key = Key::random();
        // Create a new `SwornDisk` then do some writes (with cache enabled)
        let sworndisk = SwornDisk::create(mem_disk.clone(), root_key, None, true)?;
        let num_rw = 1024;

        // Submit a write block I/O request
        let mut wbuf = Buf::alloc(num_rw)?;
        let bufs = {
            let mut bufs = Vec::with_capacity(num_rw);
            for i in 0..num_rw {
                let buf_slice = &mut wbuf.as_mut_slice()[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE];
                buf_slice.fill(i as u8);
                bufs.push(unsafe {
                    BlockBuf::from_raw_parts(
                        NonNull::new(buf_slice.as_mut_ptr()).unwrap(),
                        BLOCK_SIZE,
                    )
                });
            }
            bufs
        };
        let bio_req = BioReqBuilder::new(BioType::Write)
            .addr(0 as BlockId)
            .bufs(bufs)
            .build();
        sworndisk.submit_bio_sync(bio_req)?;

        // Sync the `SwornDisk` then do some reads
        sworndisk.submit_bio_sync(BioReqBuilder::new(BioType::Sync).build())?;

        let mut rbuf = Buf::alloc(1)?;
        for i in 0..num_rw {
            sworndisk.read(i as Lba, rbuf.as_mut())?;
            assert_eq!(rbuf.as_slice()[0], i as u8);
        }

        // Open the closed `SwornDisk` then test its data's existence
        drop(sworndisk);
        thread::spawn(move || -> Result<()> {
            let opened_sworndisk = SwornDisk::open(mem_disk, root_key, None, true)?;
            let mut rbuf = Buf::alloc(2)?;
            opened_sworndisk.read(5 as Lba, rbuf.as_mut())?;
            assert_eq!(rbuf.as_slice()[0], 5u8);
            assert_eq!(rbuf.as_slice()[4096], 6u8);
            Ok(())
        })
        .join()
        .unwrap()
    }
}
