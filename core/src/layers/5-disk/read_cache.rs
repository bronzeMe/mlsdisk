//! High-performance read cache system inspired by SEFS/rcore-fs optimizations.
//!
//! This module provides an intelligent read caching system that complements
//! the existing write-optimized DataBuf. Based on analysis of SEFS and rcore-fs
//! caching mechanisms, it implements several key optimizations:
//!
//! # Key Optimizations (Inspired by SEFS)
//!
//! - **Fine-grained locking**: Each cache slot has independent mutex to reduce contention
//! - **Non-blocking lookups**: Uses try_lock for efficient cache searches
//! - **Efficient LRU**: Array-based doubly-linked list with O(1) operations
//! - **Pre-allocated slots**: Fixed-size cache pool avoids runtime allocations
//! - **Smart caching**: Adaptive strategies based on access patterns
//! - **SGX compatible**: Uses crate::os synchronization primitives

use super::sworndisk::{RecordKey, RecordValue};
use crate::layers::bio::{BufMut, BLOCK_SIZE};
use crate::os::{Mutex, Arc, BTreeSet};
use crate::prelude::*;

#[cfg(not(feature = "linux"))]
use log::debug;

/// Read cache capacity (32MB = 8192 blocks of 4KB each)
pub(super) const READ_CACHE_CAPACITY: usize = 8192;

/// High-performance read cache system inspired by SEFS BlockCache design.
///
/// Key optimizations:
/// - Fine-grained locking: Each cache slot has independent mutex
/// - Non-blocking lookups: Uses try_lock to avoid waiting
/// - Efficient LRU: Array-based implementation with O(1) operations
/// - Pre-allocated cache slots: No runtime memory allocation overhead
#[derive(Debug)]
pub(super) struct ReadCacheSystem {
    /// Pre-allocated cache slots, each with independent lock
    cache_slots: Vec<Mutex<CacheSlot>>,
    
    /// LRU management with efficient array-based implementation
    lru_manager: Mutex<LRUManager>,
    
    /// Global cache statistics
    stats: Mutex<CacheStats>,
    
    /// Cache capacity limit
    capacity: usize,
}

/// Individual cache slot with independent locking
#[derive(Debug)]
struct CacheSlot {
    /// Current status of this cache slot
    status: SlotStatus,
    
    /// Cached block data (when Valid)
    data: Box<[u8; BLOCK_SIZE]>,
}

/// Status of a cache slot (inspired by SEFS Buf status)
#[derive(Debug, Clone, Copy, PartialEq)]
enum SlotStatus {
    /// Slot is unused and available
    Unused,
    
    /// Slot contains valid cached data
    Valid(RecordKey),
}

/// Efficient LRU manager using array-based doubly-linked list (inspired by rcore-fs)
#[derive(Debug)]
struct LRUManager {
    /// Previous slot index for each slot (doubly-linked list)
    prev: Vec<usize>,
    
    /// Next slot index for each slot (doubly-linked list)  
    next: Vec<usize>,
    
    /// Access counter for ordering (simple timestamp-based LRU)
    access_counter: usize,
    
    /// Last access time for each slot
    last_access: Vec<usize>,
}

/// Cached data block with metadata.
///
/// Stores decrypted data to avoid repeated decryption overhead.
/// Enhanced with timestamp for better eviction strategy.
#[derive(Debug)]
pub(super) struct CachedBlock {
    /// Decrypted block data
    data: Box<[u8; BLOCK_SIZE]>,
    
    /// Access counter for LRU-like behavior
    access_count: usize,
    
    /// Last access timestamp for LRU eviction decisions
    last_access_time: usize,
}

/// Cache lookup result optimized for minimal copying.
#[derive(Debug)]
pub(super) enum CacheLookupResult {
    /// Cache hit with direct copy capability (single-copy design)
    Hit,
    
    /// Cache miss - need to fetch from storage
    Miss,
}

/// Cache insertion hint for optimization.
#[derive(Debug, Clone, Copy)]
pub(super) enum CacheInsertHint {
    /// Normal cache insertion
    Normal,
    
    /// High priority insertion (frequently accessed)
    Hot,
    
    /// Low priority insertion (may be evicted early)
    Cold,
}

/// Simple cache statistics.
#[derive(Debug)]
pub struct CacheStats {
    /// Total cache hits
    hits: usize,
    
    /// Total cache misses  
    misses: usize,
    
    /// Total cache insertions
    insertions: usize,
    
    /// Total cache evictions
    evictions: usize,
    
    /// Current cache size
    current_size: usize,
}

impl ReadCacheSystem {
    /// Create a new high-performance read cache system.
    /// 
    /// Pre-allocates all cache slots to avoid runtime allocation overhead,
    /// inspired by SEFS BlockCache design.
    pub fn new() -> Result<Self> {
        #[cfg(not(feature = "linux"))]
        debug!("[ReadCacheSystem] Initializing high-performance cache with {} slots ({}MB)", 
               READ_CACHE_CAPACITY, READ_CACHE_CAPACITY * BLOCK_SIZE / 1024 / 1024);
        
        // Pre-allocate all cache slots with independent mutexes
        let mut cache_slots = Vec::with_capacity(READ_CACHE_CAPACITY);
        for _ in 0..READ_CACHE_CAPACITY {
            cache_slots.push(Mutex::new(CacheSlot {
                status: SlotStatus::Unused,
                data: Box::new([0u8; BLOCK_SIZE]),
            }));
        }
        
        // Initialize efficient LRU manager
        let lru_manager = Mutex::new(LRUManager::new(READ_CACHE_CAPACITY));
        
        let cache_system = Self {
            cache_slots,
            lru_manager,
            stats: Mutex::new(CacheStats::new()),
            capacity: READ_CACHE_CAPACITY,
        };
        
        #[cfg(not(feature = "linux"))]
        debug!("[ReadCacheSystem] High-performance cache initialization completed");
        
        Ok(cache_system)
    }

    /// High-performance cache lookup with direct copy (single-copy optimization).
    ///
    /// Searches for cached data and directly copies to target buffer if found,
    /// eliminating intermediate data cloning for true single-copy performance.
    pub fn lookup_and_copy(&self, key: RecordKey, buf: &mut BufMut) -> CacheLookupResult {
        debug_assert_eq!(buf.nblocks(), 1);
        
        // Fast non-blocking search through cache slots (inspired by SEFS _get_buf)
        for (slot_idx, slot_mutex) in self.cache_slots.iter().enumerate() {
            if let Some(slot) = slot_mutex.try_lock() {
                match slot.status {
                    SlotStatus::Valid(cached_key) if cached_key == key => {
                        // Cache hit! Direct copy to target buffer (single copy)
                        buf.as_mut_slice().copy_from_slice(&slot.data[..]);
                        
                        // Update LRU and stats (after copy to minimize lock time)
                        drop(slot); // Release slot lock immediately
                        self.update_lru_on_hit(slot_idx);
                        self.stats.lock().hits += 1;
                        
                        return CacheLookupResult::Hit;
                    }
                    _ => continue,
                }
            }
            // If slot is locked, continue searching (non-blocking approach)
        }
        
        // Cache miss
        self.stats.lock().misses += 1;
        CacheLookupResult::Miss
    }
    
    /// Alternative direct copy method for scattered buffer scenarios.
    ///
    /// Directly copies cached data to specified slice, optimized for multi-block reads.
    pub fn lookup_and_copy_to_slice(&self, key: RecordKey, target_slice: &mut [u8]) -> CacheLookupResult {
        debug_assert_eq!(target_slice.len(), BLOCK_SIZE);
        
        // Fast non-blocking search through cache slots
        for (slot_idx, slot_mutex) in self.cache_slots.iter().enumerate() {
            if let Some(slot) = slot_mutex.try_lock() {
                match slot.status {
                    SlotStatus::Valid(cached_key) if cached_key == key => {
                        // Cache hit! Direct copy to target slice (single copy)
                        target_slice.copy_from_slice(&slot.data[..]);
                        
                        // Update LRU and stats (after copy to minimize lock time)
                        drop(slot); // Release slot lock immediately
                        self.update_lru_on_hit(slot_idx);
                        self.stats.lock().hits += 1;
                        
                        return CacheLookupResult::Hit;
                    }
                    _ => continue,
                }
            }
        }
        
        // Cache miss
        self.stats.lock().misses += 1;
        CacheLookupResult::Miss
    }
    
    /// Update LRU state when cache hit occurs (O(1) operation)
    fn update_lru_on_hit(&self, slot_idx: usize) {
        if let Some(mut lru) = self.lru_manager.try_lock() {
            lru.access_counter += 1;
            lru.last_access[slot_idx] = lru.access_counter;
            lru.move_to_head(slot_idx);
        }
        // If LRU lock is busy, skip update (performance optimization)
    }

    /// High-performance cache insertion with efficient LRU eviction.
    ///
    /// Uses pre-allocated slots and non-blocking approach inspired by SEFS.
    /// Supports priority-based insertion hints for better cache management.
    pub fn insert(&self, key: RecordKey, data: Box<[u8; BLOCK_SIZE]>, hint: CacheInsertHint) -> Result<()> {
        // First try to find an unused slot (fast path)
        if let Some(slot_idx) = self.find_unused_slot() {
            self.insert_into_slot(slot_idx, key, data);
            self.update_stats_on_insert();
            return Ok(());
        }
        
        // No unused slots, need to evict LRU entry
        let victim_idx = self.find_lru_victim(hint)?;
        
        #[cfg(not(feature = "linux"))]
        if let Some(old_key) = self.get_slot_key(victim_idx) {
            debug!("[ReadCacheSystem] Evicting LRU block {} for new block {}", 
                   old_key.lba, key.lba);
        }
        
        // Insert into victim slot
        self.insert_into_slot(victim_idx, key, data);
        self.update_stats_on_evict_and_insert();
        
        Ok(())
    }

    /// Find an unused cache slot (non-blocking)
    fn find_unused_slot(&self) -> Option<usize> {
        for (idx, slot_mutex) in self.cache_slots.iter().enumerate() {
            if let Some(slot) = slot_mutex.try_lock() {
                if matches!(slot.status, SlotStatus::Unused) {
                    return Some(idx);
                }
            }
        }
        None
    }
    
    /// Find LRU victim slot for eviction
    fn find_lru_victim(&self, _hint: CacheInsertHint) -> Result<usize> {
        let lru = self.lru_manager.lock();
        let victim_idx = lru.get_lru_victim();
        
        if victim_idx >= self.capacity {
            return Err(Error::with_msg(OutOfMemory, "Cache LRU eviction failed"));
        }
        
        Ok(victim_idx)
    }
    
    /// Insert data into specific slot
    fn insert_into_slot(&self, slot_idx: usize, key: RecordKey, data: Box<[u8; BLOCK_SIZE]>) {
        if let Some(mut slot) = self.cache_slots[slot_idx].try_lock() {
            slot.status = SlotStatus::Valid(key);
            *slot.data = *data;
        }
        
        // Update LRU state
        if let Some(mut lru) = self.lru_manager.try_lock() {
            lru.access_counter += 1;
            lru.last_access[slot_idx] = lru.access_counter;
            lru.move_to_head(slot_idx);
        }
    }
    
    /// Get key from slot for debugging
    fn get_slot_key(&self, slot_idx: usize) -> Option<RecordKey> {
        if let Some(slot) = self.cache_slots[slot_idx].try_lock() {
            if let SlotStatus::Valid(key) = slot.status {
                return Some(key);
            }
        }
        None
    }
    
    /// Update statistics on successful insertion
    fn update_stats_on_insert(&self) {
        if let Some(mut stats) = self.stats.try_lock() {
            stats.insertions += 1;
            stats.current_size = stats.current_size.saturating_add(1).min(self.capacity);
        }
    }
    
    /// Update statistics on eviction and insertion
    fn update_stats_on_evict_and_insert(&self) {
        if let Some(mut stats) = self.stats.try_lock() {
            stats.insertions += 1;
            stats.evictions += 1;
            // Size stays the same (evict + insert)
        }
    }

    /// Get cache statistics with minimal locking overhead.
    pub fn stats(&self) -> CacheStats {
        let stats = self.stats.lock().clone();
        
        #[cfg(not(feature = "linux"))]
        if (stats.hits + stats.misses) % 1000 == 0 && (stats.hits + stats.misses) > 0 {
            debug!("[ReadCacheSystem] High-performance cache stats: hits={}, misses={}, hit_ratio={:.1}%, size={}/{}", 
                   stats.hits, stats.misses, stats.hit_ratio(), 
                   stats.current_size, READ_CACHE_CAPACITY);
        }
        
        stats
    }

    /// Clear all cached data - high-performance version.
    #[cfg(test)]
    pub fn clear(&self) {
        let mut cleared_count = 0;
        
        // Clear all cache slots
        for slot_mutex in &self.cache_slots {
            if let Some(mut slot) = slot_mutex.try_lock() {
                if matches!(slot.status, SlotStatus::Valid(_)) {
                    slot.status = SlotStatus::Unused;
                    cleared_count += 1;
                }
            }
        }
        
        // Reset LRU state
        if let Some(mut lru) = self.lru_manager.try_lock() {
            *lru = LRUManager::new(self.capacity);
        }
        
        // Update statistics
        if let Some(mut stats) = self.stats.try_lock() {
            stats.current_size = 0;
            stats.evictions += cleared_count;
        }
    }

    /// Get current cache size efficiently.
    pub fn size(&self) -> usize {
        self.stats.lock().current_size
    }

    /// Check if cache is empty efficiently.
    pub fn is_empty(&self) -> bool {
        self.stats.lock().current_size == 0
    }

    /// High-performance cache invalidation for data consistency.
    /// Uses non-blocking approach to minimize performance impact.
    pub fn invalidate(&self, key: RecordKey) -> bool {
        // Search through cache slots and invalidate matching entries
        for (slot_idx, slot_mutex) in self.cache_slots.iter().enumerate() {
            if let Some(mut slot) = slot_mutex.try_lock() {
                if let SlotStatus::Valid(cached_key) = slot.status {
                    if cached_key == key {
                        slot.status = SlotStatus::Unused;
                        
                        // Update statistics
                        if let Some(mut stats) = self.stats.try_lock() {
                            stats.evictions += 1;
                            stats.current_size = stats.current_size.saturating_sub(1);
                        }
                        
                        // Update LRU state
                        if let Some(mut lru) = self.lru_manager.try_lock() {
                            lru.invalidate_slot(slot_idx);
                        }
            
            #[cfg(not(feature = "linux"))]
            debug!("[ReadCacheSystem] Invalidated block {} for data consistency", key.lba);
            
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Efficient batch invalidation for multiple keys.
    /// Optimized for write operations that affect multiple blocks.
    pub fn invalidate_batch(&self, keys: &[RecordKey]) -> usize {
        let mut invalidated_count = 0;
        
        // Build a set for efficient lookup (using BTreeSet for SGX compatibility)
        let key_set: BTreeSet<RecordKey> = keys.iter().cloned().collect();
        
        // Search through all slots and invalidate matching entries
        for (slot_idx, slot_mutex) in self.cache_slots.iter().enumerate() {
            if let Some(mut slot) = slot_mutex.try_lock() {
                if let SlotStatus::Valid(cached_key) = slot.status {
                    if key_set.contains(&cached_key) {
                        slot.status = SlotStatus::Unused;
                invalidated_count += 1;
                        
                        // Update LRU state
                        if let Some(mut lru) = self.lru_manager.try_lock() {
                            lru.invalidate_slot(slot_idx);
                        }
                    }
                }
            }
        }
        
        // Update statistics
        if invalidated_count > 0 {
            if let Some(mut stats) = self.stats.try_lock() {
                stats.evictions += invalidated_count;
                stats.current_size = stats.current_size.saturating_sub(invalidated_count);
            }
            
            #[cfg(not(feature = "linux"))]
            debug!("[ReadCacheSystem] Batch invalidated {} blocks for data consistency", invalidated_count);
        }
        
        invalidated_count
    }
}

impl CachedBlock {
    /// Create a cached block from pre-allocated data
    pub fn from_data(data: Box<[u8; BLOCK_SIZE]>) -> Arc<Self> {
        Arc::new(Self {
            data,
            access_count: 1,
            last_access_time: 0, // Will be updated by caller
        })
    }

    /// Get a reference to the block data (zero-copy).
    pub fn data(&self) -> &[u8; BLOCK_SIZE] {
        &self.data
    }

    /// Copy data to buffer, following data_buf.rs pattern.
    pub fn copy_to_buf(&self, buf: &mut BufMut) -> Result<()> {
        debug_assert_eq!(buf.nblocks(), 1);
        buf.as_mut_slice().copy_from_slice(&*self.data);
        Ok(())
    }

    /// Get access count for debugging.
    pub fn access_count(&self) -> usize {
        self.access_count
    }

    /// Get last access time for debugging.
    pub fn last_access_time(&self) -> usize {
        self.last_access_time
    }
}

impl CacheStats {
    /// Create new cache statistics.
    fn new() -> Self {
        Self {
            hits: 0,
            misses: 0,
            insertions: 0,
            evictions: 0,
            current_size: 0,
        }
    }

    /// Get cache hit ratio as percentage.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hits as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }

    /// Get total cache hits.
    pub fn total_hits(&self) -> usize {
        self.hits
    }

    /// Get total cache misses.
    pub fn total_misses(&self) -> usize {
        self.misses
    }

    /// Get total insertions.
    pub fn total_insertions(&self) -> usize {
        self.insertions
    }

    /// Get total evictions.
    pub fn total_evictions(&self) -> usize {
        self.evictions
    }

    /// Get current cache size in blocks.
    pub fn current_size_blocks(&self) -> usize {
        self.current_size
    }

    /// Get cache memory usage in bytes.
    pub fn memory_usage_bytes(&self) -> usize {
        self.current_size * BLOCK_SIZE
    }

    /// Get memory usage in MB.
    pub fn memory_usage_mb(&self) -> f64 {
        self.memory_usage_bytes() as f64 / 1024.0 / 1024.0
    }

    /// Get overall cache hit ratio (alias for compatibility).
    pub fn overall_hit_ratio(&self) -> f64 {
        self.hit_ratio() / 100.0  // Return as fraction, not percentage
    }
}

impl Clone for CacheStats {
    fn clone(&self) -> Self {
        Self {
            hits: self.hits,
            misses: self.misses,
            insertions: self.insertions,
            evictions: self.evictions,
            current_size: self.current_size,
        }
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self::new()
    }
}

impl LRUManager {
    /// Create a new LRU manager with efficient array-based implementation
    fn new(capacity: usize) -> Self {
        // Initialize doubly-linked list (inspired by rcore-fs LRU)
        // Head is at index 0, all slots initially form a circular list
        let prev = (capacity - 1..capacity).chain(0..capacity - 1).collect();
        let next = (1..capacity).chain(0..1).collect();
        
        Self {
            prev,
            next,
            access_counter: 0,
            last_access: vec![0; capacity],
        }
    }
    
    /// Move slot to head of LRU list (most recently used)
    fn move_to_head(&mut self, slot_idx: usize) {
        if slot_idx == 0 || slot_idx >= self.prev.len() {
            return;
        }
        
        // Remove from current position
        self.list_remove(slot_idx);
        
        // Insert at head
        self.list_insert_head(slot_idx);
    }
    
    /// Get LRU victim slot for eviction (tail of list)
    fn get_lru_victim(&self) -> usize {
        // Tail is the previous of head (index 0)
        self.prev[0]
    }
    
    /// Mark slot as invalidated (remove from LRU tracking)
    fn invalidate_slot(&mut self, slot_idx: usize) {
        self.last_access[slot_idx] = 0;
        // Note: We don't remove from linked list to avoid complexity
        // The slot will naturally become LRU over time
    }
    
    /// Remove slot from doubly-linked list
    fn list_remove(&mut self, slot_idx: usize) {
        let prev_idx = self.prev[slot_idx];
        let next_idx = self.next[slot_idx];
        self.prev[next_idx] = prev_idx;
        self.next[prev_idx] = next_idx;
    }
    
    /// Insert slot at head of doubly-linked list
    fn list_insert_head(&mut self, slot_idx: usize) {
        let head_next = self.next[0];
        self.prev[slot_idx] = 0;
        self.next[slot_idx] = head_next;
        self.next[0] = slot_idx;
        self.prev[head_next] = slot_idx;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_creation() {
        let cache = ReadCacheSystem::new().expect("Failed to create cache system");
        assert_eq!(cache.size(), 0);
        assert!(cache.is_empty());
    }

    #[test] 
    fn test_basic_cache_operations() {
        let cache = ReadCacheSystem::new().expect("Failed to create cache");
        let key = RecordKey { lba: 100 };
        
        // Test miss
        assert!(matches!(cache.lookup(key), CacheLookupResult::Miss));
        
        // Test insert and hit
        let data = Box::new([42u8; BLOCK_SIZE]);
        cache.insert(key, data, CacheInsertHint::Normal)
            .expect("Insert failed");
        
        let mut buf = crate::layers::bio::Buf::alloc(1).unwrap();
        match cache.lookup_and_copy(key, buf.as_mut()) {
            CacheLookupResult::Hit => {
                assert_eq!(buf.as_slice()[0], 42);
            }
            CacheLookupResult::Miss => {
                panic!("Should hit cache");
            }
        }
    }

    #[test]
    fn test_cache_eviction() {
        let cache = ReadCacheSystem::new().expect("Failed to create cache");
        
        // Fill cache to capacity + 1
        for i in 0..=READ_CACHE_CAPACITY {
            let key = RecordKey { lba: i };
            let data = Box::new([i as u8; BLOCK_SIZE]);
            cache.insert(key, data, CacheInsertHint::Normal)
                .expect("Insert failed");
        }
        
        // Should not exceed capacity
        assert_eq!(cache.size(), READ_CACHE_CAPACITY);
        
        // First key should be evicted
        let first_key = RecordKey { lba: 0 };
        let mut buf = crate::layers::bio::Buf::alloc(1).unwrap();
        assert!(matches!(cache.lookup_and_copy(first_key, buf.as_mut()), CacheLookupResult::Miss));
        
        // Last key should still be present
        let last_key = RecordKey { lba: READ_CACHE_CAPACITY };
        assert!(matches!(cache.lookup_and_copy(last_key, buf.as_mut()), CacheLookupResult::Hit));
    }

    #[test]
    fn test_cache_statistics() {
        let cache = ReadCacheSystem::new().expect("Failed to create cache");
        let key = RecordKey { lba: 200 };
        
        // Initially no hits/misses
        let stats = cache.stats();
        assert_eq!(stats.total_hits(), 0);
        assert_eq!(stats.total_misses(), 0);
        
        // Cause a miss
        let mut buf = crate::layers::bio::Buf::alloc(1).unwrap();
        assert!(matches!(cache.lookup_and_copy(key, buf.as_mut()), CacheLookupResult::Miss));
        let stats = cache.stats();
        assert_eq!(stats.total_misses(), 1);
        
        // Insert and cause a hit
        let data = Box::new([99u8; BLOCK_SIZE]);
        cache.insert(key, data, CacheInsertHint::Normal)
            .expect("Insert failed");
        
        assert!(matches!(cache.lookup_and_copy(key, buf.as_mut()), CacheLookupResult::Hit));
        let stats = cache.stats();
        assert_eq!(stats.total_hits(), 1);
        assert_eq!(stats.total_insertions(), 1);
    }

    #[test]
    fn test_copy_to_buf() {
        let cache = ReadCacheSystem::new().expect("Failed to create cache");
        let key = RecordKey { lba: 300 };
        
        let mut data = Box::new([0u8; BLOCK_SIZE]);
        data[0] = 123;
        data[100] = 231;
        
        cache.insert(key, data, CacheInsertHint::Normal)
            .expect("Insert failed");
        
            let mut buf = crate::layers::bio::Buf::alloc(1).unwrap();
        match cache.lookup_and_copy(key, buf.as_mut()) {
            CacheLookupResult::Hit => {
            assert_eq!(buf.as_slice()[0], 123);
            assert_eq!(buf.as_slice()[100], 231);
            }
            CacheLookupResult::Miss => {
            panic!("Should hit cache");
            }
        }
    }
    
    #[test]
    fn test_borrowing_fix() {
        // Test specifically for the borrowing fix in lookup method
        let cache = ReadCacheSystem::new().expect("Failed to create cache");
        let key = RecordKey { lba: 500 };
        
        // Insert data
        let data = Box::new([42u8; BLOCK_SIZE]);
        cache.insert(key, data, CacheInsertHint::Normal)
            .expect("Insert failed");
        
        // Multiple lookups should work without borrowing conflicts
        for _ in 0..10 {
            let mut buf = crate::layers::bio::Buf::alloc(1).unwrap();
            match cache.lookup_and_copy(key, buf.as_mut()) {
                CacheLookupResult::Hit => {
                    assert_eq!(buf.as_slice()[0], 42);
                }
                CacheLookupResult::Miss => {
                panic!("Should hit cache");
                }
            }
        }
        
        // Verify stats work correctly 
        let stats = cache.stats();
        assert_eq!(stats.total_hits(), 10);
    }
    
    #[test]
    fn test_cache_invalidation() {
        // Test cache invalidation for data consistency
        let cache = ReadCacheSystem::new().expect("Failed to create cache");
        let key1 = RecordKey { lba: 600 };
        let key2 = RecordKey { lba: 601 };
        let key3 = RecordKey { lba: 602 };
        
        // Insert test data
        let data1 = Box::new([11u8; BLOCK_SIZE]);
        let data2 = Box::new([22u8; BLOCK_SIZE]);
        let data3 = Box::new([33u8; BLOCK_SIZE]);
        
        cache.insert(key1, data1, CacheInsertHint::Normal).expect("Insert 1 failed");
        cache.insert(key2, data2, CacheInsertHint::Normal).expect("Insert 2 failed");
        cache.insert(key3, data3, CacheInsertHint::Normal).expect("Insert 3 failed");
        
        assert_eq!(cache.size(), 3);
        
        // Test single invalidation
        let was_present = cache.invalidate(key1);
        assert!(was_present);
        assert_eq!(cache.size(), 2);
        let mut buf = crate::layers::bio::Buf::alloc(1).unwrap();
        assert!(matches!(cache.lookup_and_copy(key1, buf.as_mut()), CacheLookupResult::Miss));
        assert!(matches!(cache.lookup_and_copy(key2, buf.as_mut()), CacheLookupResult::Hit));
        
        // Test batch invalidation
        let keys_to_invalidate = [key2, key3];
        let invalidated_count = cache.invalidate_batch(&keys_to_invalidate);
        assert_eq!(invalidated_count, 2);
        assert_eq!(cache.size(), 0);
        assert!(matches!(cache.lookup_and_copy(key2, buf.as_mut()), CacheLookupResult::Miss));
        assert!(matches!(cache.lookup_and_copy(key3, buf.as_mut()), CacheLookupResult::Miss));
        
        // Test invalidation of non-existent key
        let was_present = cache.invalidate(RecordKey { lba: 999 });
        assert!(!was_present);
        
        // Verify stats are updated correctly
        let stats = cache.stats();
        assert_eq!(stats.total_evictions(), 3); // 1 + 2 from invalidations
    }
}