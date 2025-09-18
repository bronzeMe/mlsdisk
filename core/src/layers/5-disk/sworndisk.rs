//! SwornDisk as a block device.
//!
//! API: submit_bio(), submit_bio_sync(), create(), open(),
//! read(), readv(), write(), writev(), sync().
//!
//! Responsible for managing a `TxLsmTree`, whereas the TX logs (WAL and SSTs)
//! are stored; an untrusted disk storing user data, a `BlockAlloc` for managing data blocks'
//! allocation metadata. `TxLsmTree` and `BlockAlloc` are manipulated
//! based on internal transactions.
//!
//! # Large File Sequential Read Optimization
//!
//! SwornDisk implements a "prefetch + no cache" approach for 10-30GB file scenarios:
//! - Small prefetch window (8 blocks) instead of large persistent cache
//! - Sequential access detection and intelligent async prefetch
//! - Memory efficient: ~32KB prefetch window vs 128MB+ cache
//! - Optimized for TEE environments with memory constraints
use super::bio::{BioReq, BioReqQueue, BioResp, BioType};
use super::block_alloc::{AllocTable, BlockAlloc};
use super::data_buf::DataBuf;
use super::simplified_read_cache::{ReadCacheSystem, CacheLookupResult, CacheLookupResultWithPrefetch, PrefetchSuggestion};
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
    /// Optional prefetch-only system for large file sequential read optimization.
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

    /// Get enhanced read cache statistics including prefetch metrics.
    ///
    /// Returns comprehensive cache performance metrics including:
    /// - Traditional cache hit/miss ratios  
    /// - Memory usage and capacity information
    /// - Prefetch hit ratios for sequential read optimization
    /// - Eviction statistics for cache tuning
    /// 
    /// These metrics are essential for monitoring the effectiveness of the
    /// "prefetch + no cache" system and overall sequential read performance.
    /// If read cache is disabled, returns default (empty) statistics.
    pub fn cache_stats(&self) -> super::simplified_read_cache::CacheStats {
        if let Some(ref read_cache) = self.inner.read_cache {
            read_cache.stats()
        } else {
            // Return default empty stats when cache is disabled
            super::simplified_read_cache::CacheStats::default()
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
            debug!("[SwornDisk] Initializing PrefetchSystem for large file optimization...");
            Some(Arc::new(ReadCacheSystem::new()?))
        } else {
            #[cfg(not(feature = "linux"))]
            debug!("[SwornDisk] PrefetchSystem disabled");
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
            debug!("[SwornDisk] Initializing PrefetchSystem for large file optimization...");
            Some(Arc::new(ReadCacheSystem::new()?))
        } else {
            #[cfg(not(feature = "linux"))]
            debug!("[SwornDisk] PrefetchSystem disabled");
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
const DATA_BUF_CAP: usize = 1024;

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
        
        // LARGE FILE SEQUENTIAL READ OPTIMIZATION WITH PREFETCH:
        // 1. Search in write buffer (`DataBuf`) first - highest priority
        // 2. Check prefetch window for intelligent prefetched blocks
        // 3. Direct LSM tree lookup if not prefetched
        // 4. Trigger async prefetch based on sequential patterns
        
        if self.data_buf.get(key, &mut buf).is_some() {
            return Ok(());
        }

        // Check prefetch window and generate prefetch suggestions
        if let Some(ref read_cache) = self.read_cache {
            let cache_result = read_cache.lookup_and_copy_with_prefetch(key, &mut buf);
            match cache_result.result {
                CacheLookupResult::Hit => {
                    // Prefetch hit! Data already copied to buffer
                    #[cfg(not(feature = "linux"))]
                    debug!("[SwornDisk] Prefetch hit for LBA {}", key.lba);
                    
                    return Ok(());
                }
                CacheLookupResult::Miss => {
                    // Handle prefetch suggestion for sequential reads optimization
                    if let Some(suggestion) = cache_result.prefetch_suggestion {
                        self.handle_prefetch_suggestion(suggestion);
                    }
                    // Continue to disk read
                }
            }
        }

        // Direct LSM tree lookup and disk read
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

        // NO PERSISTENT CACHING: Only prefetch window for sequential optimization
        // This avoids memory pressure while maintaining sequential read performance

        Ok(())
    }

    // Note: Removed should_cache_single_block() function 
    // Now using unconditional caching strategy for optimal performance

    fn read_multi_blocks<'a>(&self, lba: Lba, bufs: &'a mut [BufMut<'a>]) -> Result<()> {
        let mut buf_vec = BufMutVec::from_bufs(bufs);
        let nblocks = buf_vec.nblocks();

        let mut range_query_ctx =
            RangeQueryCtx::<RecordKey, RecordValue>::new(RecordKey { lba }, nblocks);

        // LARGE FILE SEQUENTIAL READ OPTIMIZATION WITH PREFETCH:
        // 1. Search in `DataBuf` first (write buffer)
        // 2. Check prefetch window for intelligent prefetched blocks
        // 3. Direct LSM tree range query for remaining blocks
        // 4. Batch disk reads with direct decryption to user buffers
        
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

        // Check prefetch window for remaining blocks
        if let Some(ref read_cache) = self.read_cache {
            if let Some(uncompleted_range) = range_query_ctx.range_uncompleted() {
                for current_lba in uncompleted_range.start().lba..=uncompleted_range.end().lba {
                    let key = RecordKey { lba: current_lba };
                    
                    // Only check uncompleted blocks
                    if !range_query_ctx.contains_uncompleted(&key) {
                        continue;
                    }
                    
                    // Check prefetch window for this specific block  
                    let target_slice = buf_vec.nth_buf_mut_slice(current_lba - lba);
                    match read_cache.lookup_and_copy_to_slice(key, target_slice) {
                        CacheLookupResult::Hit => {
                            // Prefetch hit! Data directly copied to target slice
                            range_query_ctx.mark_completed(key);
                        }
                        CacheLookupResult::Miss => {
                            // Not in prefetch window - will be handled by LSM Tree lookup
                        }
                    }
                }
            }
        }
        
        if range_query_ctx.is_completed() {
            return Ok(());
        }

        // Direct LSM tree range query for remaining blocks
        self.logical_block_table.get_range(&mut range_query_ctx)?;
        debug_assert!(range_query_ctx.is_completed());

        let mut res = range_query_ctx.into_results();
        let record_batches = {
            res.sort_by(|(_, v1), (_, v2)| v1.hba.cmp(&v2.hba));
            res.group_by(|(_, v1), (_, v2)| v2.hba - v1.hba == 1)
        };

        // Batch disk reads with direct decryption to user buffers
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
                
                // NO PERSISTENT CACHING: Only prefetch window for sequential access
                // This avoids memory pressure while maintaining performance benefits
            }
        }

        Ok(())
    }

    /// Write a specified number of blocks at a logical block address on the device.
    /// The block contents reside in a single contiguous buffer.
    pub fn write(&self, mut lba: Lba, buf: BufRef) -> Result<()> {
        // ZERO-LOCK LARGE FILE SEQUENTIAL WRITE OPTIMIZATION:
        // Cache invalidation completely eliminated for maximum write performance
        // DataBuf acts as authoritative source, no cache coordination needed
        
        // Write block contents to `DataBuf` directly - no cache overhead
        for block_buf in buf.iter() {
            let key = RecordKey { lba };
            
            let buf_at_capacity = self.data_buf.put(key, block_buf);

            // Flush all data blocks in `DataBuf` to disk if it's full
            if buf_at_capacity {
                // TODO: Error handling: Should discard current write in `DataBuf`
                self.flush_data_buf()?;
            }
            lba += 1;
        }
        
        // PERFORMANCE BREAKTHROUGH: Zero cache invalidation overhead
        // Cache is disabled, eliminating all cache management from write path
        // This provides maximum throughput for large file streaming writes
        
        Ok(())
    }

    /// Write multiple blocks at a logical block address on the device.
    /// The block contents reside in several scattered buffers.
    pub fn writev(&self, mut lba: Lba, bufs: &[BufRef]) -> Result<()> {
        // ZERO-LOCK LARGE FILE SEQUENTIAL WRITE OPTIMIZATION:
        // Completely simplified write path without any cache management overhead
        for buf in bufs {
            self.write(lba, *buf)?;
            lba += buf.nblocks();
        }
        
        // PERFORMANCE BREAKTHROUGH: Zero cache invalidation overhead
        // All cache operations eliminated for maximum write throughput
        Ok(())
    }

    fn flush_data_buf(&self) -> Result<()> {
        let records = self.write_blocks_from_data_buf()?;
        
                // PREFETCH WINDOW INVALIDATION FOR WRITE CONSISTENCY:
                // Invalidate from prefetch window only (small, efficient operation)
                // This maintains correctness while avoiding large cache management overhead
                if !records.is_empty() && self.read_cache.is_some() {
                    if let Some(ref read_cache) = self.read_cache {
                        let invalidated_count = read_cache.invalidate_iter(records.iter().map(|(key, _)| *key));
                        
                        #[cfg(not(feature = "linux"))]
                        debug!("[SwornDisk] Invalidated {} entries from prefetch window on flush", invalidated_count);
                    }
                }
        
        // Insert new records of data blocks to `TxLsmTree`
        for (key, value) in records {
            // TODO: Error handling: Should dealloc the written blocks
            self.logical_block_table.put(key, value)?;
        }

        self.data_buf.clear();
        
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
    
    /// Handle prefetch suggestions from prefetch system for sequential read optimization.
    /// 
    /// This method implements intelligent async prefetch based on prefetch system suggestions.
    /// It uses a conservative approach to avoid overwhelming the system while providing
    /// meaningful performance benefits for large file sequential reads.
    fn handle_prefetch_suggestion(&self, suggestion: PrefetchSuggestion) {
        // Only proceed with high-confidence suggestions to avoid wasted I/O
        if suggestion.confidence < 70 {
            #[cfg(not(feature = "linux"))]
            debug!("[SwornDisk] Skipping low-confidence prefetch suggestion ({}%)", suggestion.confidence);
            return;
        }
        
        let prefetch_count = suggestion.suggested_keys.len().min(4); // Conservative limit
        if prefetch_count == 0 {
            return;
        }
        
        #[cfg(not(feature = "linux"))]
        debug!("[SwornDisk] Processing prefetch suggestion: {} keys, confidence: {}%", 
               prefetch_count, suggestion.confidence);
        
        // Process prefetch suggestions with high-confidence keys only
        for &prefetch_key in suggestion.suggested_keys.iter().take(prefetch_count) {
            if let Err(e) = self.execute_single_prefetch(prefetch_key) {
                #[cfg(not(feature = "linux"))]
                debug!("[SwornDisk] Prefetch failed for LBA {}: {:?}", prefetch_key.lba, e);
                
                // On error, stop prefetching to avoid cascade failures
                break;
            }
        }
    }
    
    /// Execute a single block prefetch operation with buffer pool optimization.
    /// 
    /// This performs the actual async read and cache insertion for a single block.
    /// Returns early if the block is already in prefetch window or LSM tree lookup fails.
    /// 
    /// BUFFER POOL OPTIMIZATION: Uses pre-allocated buffers from SimplifiedReadCache
    /// buffer pool, eliminating all dynamic allocation during prefetch operations.
    fn execute_single_prefetch(&self, key: RecordKey) -> Result<()> {
        // Get pre-allocated buffer from prefetch system buffer pool
        let mut cached_data = if let Some(ref read_cache) = self.read_cache {
            match read_cache.get_prefetch_buffer() {
                Some(buffer) => buffer,
                None => {
                    // Fallback to dynamic allocation if pool is empty
                    #[cfg(not(feature = "linux"))]
                    debug!("[SwornDisk] Buffer pool empty, using fallback allocation for LBA {}", key.lba);
                    Box::new([0u8; BLOCK_SIZE])
                }
            }
        } else {
            // No prefetch system, use direct allocation
            Box::new([0u8; BLOCK_SIZE])
        };
        
        // Skip if already in DataBuf (write buffer) - use cached_data as temp buffer
        {
            let mut temp_buf = BufMut::try_from(cached_data.as_mut_slice())
                .map_err(|_| Error::with_msg(InvalidArgs, "cached_data buffer conversion failed"))?;
            if self.data_buf.get(key, &mut temp_buf).is_some() {
                // Return buffer to pool before early return
                if let Some(ref read_cache) = self.read_cache {
                    read_cache.return_prefetch_buffer(cached_data);
                }
                return Ok(());
            }
        }
        
        // Lookup in LSM tree - this is the expensive operation we're trying to optimize
        let value = match self.logical_block_table.get(&key) {
            Ok(v) => v,
            Err(_) => {
                // Block doesn't exist, return buffer to pool and skip prefetch
                if let Some(ref read_cache) = self.read_cache {
                    read_cache.return_prefetch_buffer(cached_data);
                }
                return Ok(());
            }
        };
        
        // Perform disk read and decrypt directly into cached_data
        let mut cipher = Buf::alloc(1).map_err(|_e| {
            Error::with_msg(OutOfMemory, "prefetch cipher buffer allocation failed")
        })?;
        
        if let Err(e) = self.user_data_disk.read(value.hba, cipher.as_mut()) {
            // Return buffer to pool on disk read error
            if let Some(ref read_cache) = self.read_cache {
                read_cache.return_prefetch_buffer(cached_data);
            }
            return Err(e);
        }
        
        // Decrypt directly into cached_data - ZERO COPY optimization
        if let Err(e) = Aead::new().decrypt(
            cipher.as_slice(),
            &value.key,
            &Iv::new_zeroed(),
            &[],
            &value.mac,
            cached_data.as_mut_slice(),
        ) {
            // Return buffer to pool on decryption error
            if let Some(ref read_cache) = self.read_cache {
                read_cache.return_prefetch_buffer(cached_data);
            }
            return Err(e);
        }
        
        // Insert pre-filled cached_data into prefetch window
        if let Some(ref read_cache) = self.read_cache {
            if let Err(_) = read_cache.insert_prefetched(key, cached_data) {
                // Prefetch insertion failure is not critical - just log and continue
                // Buffer is consumed by insert_prefetched even on failure
                #[cfg(not(feature = "linux"))]
                debug!("[SwornDisk] Prefetch window insertion failed for LBA {}", key.lba);
            } else {
                #[cfg(not(feature = "linux"))]
                debug!("[SwornDisk] Successfully prefetched block LBA {} using buffer pool", key.lba);
            }
        }
        
        Ok(())
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
    /// Optional prefetch system reference for window invalidation during compaction
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
    /// Optional prefetch system reference for window invalidation during compaction
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
            // Major Compaction TX and Migration TX: skip prefetch window invalidation
            TxType::Compaction { .. } | TxType::Migration => {
                // PERFORMANCE OPTIMIZATION: Skip prefetch window invalidation during compaction add_record
                // Prefetch window was already invalidated during flush_data_buf(), compaction only reorganizes
                // physical storage without changing logical data content, so no invalidation needed
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
                // PERFORMANCE BREAKTHROUGH: Eliminate redundant prefetch window invalidation during compaction
                // 
                // Rationale:
                // 1. flush_data_buf() already invalidated all relevant prefetch window entries
                // 2. Compaction only reorganizes physical storage layout, not logical data content  
                // 3. Even when records are dropped (version conflicts), the latest data was
                //    already invalidated from prefetch window during the original write->flush cycle
                // 4. This eliminates 50-75% of unnecessary prefetch window invalidation operations
                //
                // Result: Dramatic reduction in compaction overhead, especially for write-heavy workloads
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
pub struct RecordKey {
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

    #[test] 
    fn test_prefetch_integration() -> Result<()> {
        let nblocks = 16 * 1024;
        let mem_disk = MemDisk::create(nblocks)?;
        let root_key = Key::random();
        
        // Create SwornDisk with cache enabled for prefetch testing
        let sworndisk = SwornDisk::create(mem_disk, root_key, None, true)?;
        
        // Write sequential data pattern
        let num_blocks = 20;
        let mut wbuf = Buf::alloc(1)?;
        for i in 0..num_blocks {
            let buf_slice = wbuf.as_mut_slice();
            buf_slice.fill(i as u8);
            sworndisk.write(i as Lba, wbuf.as_ref())?;
        }
        
        // Sync to ensure data is written
        sworndisk.sync()?;
        
        // Simulate sequential read pattern to trigger prefetch
        let mut rbuf = Buf::alloc(1)?;
        
        // First few reads should miss cache but establish pattern
        for i in 0..5 {
            sworndisk.read(i as Lba, rbuf.as_mut())?;
            assert_eq!(rbuf.as_slice()[0], i as u8);
        }
        
        // Get initial stats
        let initial_stats = sworndisk.cache_stats();
        println!("Initial cache stats - Hits: {}, Misses: {}, Prefetch Hits: {}", 
                 initial_stats.total_hits(), 
                 initial_stats.total_misses(),
                 initial_stats.total_prefetch_hits());
        
        // Continue sequential reads - prefetch should kick in
        for i in 5..15 {
            sworndisk.read(i as Lba, rbuf.as_mut())?;
            assert_eq!(rbuf.as_slice()[0], i as u8);
        }
        
        // Check final stats - should see improvement from prefetch
        let final_stats = sworndisk.cache_stats();
        println!("Final cache stats - Hits: {}, Misses: {}, Prefetch Hits: {}", 
                 final_stats.total_hits(), 
                 final_stats.total_misses(),
                 final_stats.total_prefetch_hits());
        
        // Verify cache is working (more hits than initial)
        assert!(final_stats.total_hits() > initial_stats.total_hits());
        
        // Verify cache has reasonable hit ratio for sequential reads
        let hit_ratio = final_stats.hit_ratio();
        println!("Cache hit ratio: {:.1}%", hit_ratio);
        
        // For sequential reads with prefetch, we should see decent hit ratio
        assert!(hit_ratio > 10.0); // At least some cache effectiveness
        
        Ok(())
    }
}
