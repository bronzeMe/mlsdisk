//! Intelligent read cache system.
//!
//! This module provides a high-performance read caching system that complements
//! the existing write-optimized DataBuf. Inspired by data_buf.rs design patterns,
//! it uses simple but effective locking mechanisms suitable for SGX environment.
//!
//! # Design Philosophy
//!
//! - **Simple locks**: Use proven Mutex/BTreeMap pattern from data_buf.rs
//! - **SGX compatible**: Use crate::os synchronization primitives
//! - **Zero-copy reads**: Cache stores decrypted data to avoid repeated decryption
//! - **True LRU eviction**: Authentic LRU based on last access time, not insertion time
//! - **Write path isolation**: Read cache never interferes with write operations

use super::sworndisk::{RecordKey, RecordValue};
use crate::layers::bio::{BufMut, BLOCK_SIZE};
use crate::os::{BTreeMap, Mutex, Arc};
use crate::prelude::*;

#[cfg(not(feature = "linux"))]
use log::debug;


/// Read cache capacity (32MB = 8192 blocks of 4KB each)
pub(super) const READ_CACHE_CAPACITY: usize = 8192;

/// True LRU read cache system - fixes lock contention and algorithm issues.
///
/// Uses single-lock design and authentic LRU eviction based on access time.
#[derive(Debug)]
pub(super) struct ReadCacheSystem {
    /// Combined cache data and stats under single lock
    cache: Mutex<CacheData>,
    
    /// Cache capacity limit
    capacity: usize,
}

/// Cache data structure combining map and statistics under single lock
#[derive(Debug)]
struct CacheData {
    /// Main cache mapping
    map: BTreeMap<RecordKey, Arc<CachedBlock>>,
    
    /// External access time tracking (avoids Arc::get_mut issues)
    access_times: BTreeMap<RecordKey, usize>,
    
    /// Integrated statistics to avoid separate lock
    stats: CacheStats,
    
    /// Simple counter for insertion timestamps
    insert_counter: usize,
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

/// Cache lookup result.
#[derive(Debug)]
pub(super) enum CacheLookupResult {
    /// Cache hit with data reference
    Hit(Arc<CachedBlock>),
    
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
    /// Create a new optimized read cache system.
    pub fn new() -> Result<Self> {
        #[cfg(not(feature = "linux"))]
        debug!("[ReadCacheSystem] Initializing with capacity {} blocks ({}MB)", 
               READ_CACHE_CAPACITY, READ_CACHE_CAPACITY * BLOCK_SIZE / 1024 / 1024);
        
        let cache_system = Self {
            cache: Mutex::new(CacheData {
                map: BTreeMap::new(),
                access_times: BTreeMap::new(),
                stats: CacheStats::new(),
                insert_counter: 0,
            }),
            capacity: READ_CACHE_CAPACITY,
        };
        
        #[cfg(not(feature = "linux"))]
        debug!("[ReadCacheSystem] Initialization completed successfully");
        
        Ok(cache_system)
    }

    /// True LRU cache lookup - fixes dual lock contention and borrowing conflicts.
    ///
    /// Updates last access time for accurate LRU eviction strategy.
    /// Returns cached data if found, using single lock for better performance.
    pub fn lookup(&self, key: RecordKey) -> CacheLookupResult {
        let mut cache_data = self.cache.lock();
        
        if let Some(cached_block) = cache_data.map.get(&key) {
            // CRITICAL FIX: Use external access time tracking
            cache_data.insert_counter += 1;
            cache_data.access_times.insert(key, cache_data.insert_counter);
            
            let result_block = cached_block.clone();
            cache_data.stats.hits += 1;
            CacheLookupResult::Hit(result_block)
        } else {
            cache_data.stats.misses += 1;
            CacheLookupResult::Miss
        }
    }

    /// True LRU cache insertion - fixes dual lock and long-hold issues.
    ///
    /// Uses authentic LRU eviction strategy based on last access time.
    /// Provides better performance with single lock design.
    pub fn insert(&self, key: RecordKey, data: Box<[u8; BLOCK_SIZE]>, _hint: CacheInsertHint) -> Result<()> {
        let mut cache_data = self.cache.lock();
        
        // True LRU eviction: remove least recently used entry if at capacity
        if cache_data.map.len() >= self.capacity {
            #[cfg(not(feature = "linux"))]
            debug!("[ReadCacheSystem] Cache at capacity {}, performing LRU eviction", self.capacity);
            
            // Find LRU key using external access times
            if let Some((&lru_key, _)) = cache_data.access_times.iter()
                .min_by_key(|(_, &access_time)| access_time) {
                cache_data.map.remove(&lru_key);
                cache_data.access_times.remove(&lru_key);
                cache_data.stats.evictions += 1;
                
                #[cfg(not(feature = "linux"))]
                debug!("[ReadCacheSystem] Evicted LRU block {}", lru_key.lba);
            } else {
                // This should never happen if capacity > 0, but return error instead of inserting
                #[cfg(not(feature = "linux"))]
                return Err(Error::with_msg(OutOfMemory, "Cache LRU eviction failed"));
            }
        }
        
        // Create cached block and track access time
        cache_data.insert_counter += 1;
        let cached_block = Arc::new(CachedBlock::new(data, cache_data.insert_counter));
        
        cache_data.map.insert(key, cached_block);
        cache_data.access_times.insert(key, cache_data.insert_counter);
        cache_data.stats.insertions += 1;
        cache_data.stats.current_size = cache_data.map.len();
        
        Ok(())
    }

    /// Get cache statistics - optimized to avoid dual lock.
    pub fn stats(&self) -> CacheStats {
        let cache_data = self.cache.lock();
        let stats = cache_data.stats.clone();
        
        #[cfg(not(feature = "linux"))]
        if (stats.hits + stats.misses) % 1000 == 0 && (stats.hits + stats.misses) > 0 {
            debug!("[ReadCacheSystem] Stats: hits={}, misses={}, hit_ratio={:.1}%, size={}/{}", 
                   stats.hits, stats.misses, stats.hit_ratio(), 
                   stats.current_size, READ_CACHE_CAPACITY);
        }
        
        stats
    }

    /// Clear all cached data - optimized single lock version.
    #[cfg(test)]
    pub fn clear(&self) {
        let mut cache_data = self.cache.lock();
        let cleared_count = cache_data.map.len();
        cache_data.map.clear();
        cache_data.access_times.clear();  // Also clear access time tracking
        cache_data.stats.current_size = 0;
        // Optionally track cleared entries as evictions for testing consistency
        cache_data.stats.evictions += cleared_count;
    }

    /// Get current cache size - optimized single lock version.
    pub fn size(&self) -> usize {
        self.cache.lock().map.len()
    }

    /// Check if cache is empty - optimized single lock version.
    pub fn is_empty(&self) -> bool {
        self.cache.lock().map.is_empty()
    }

    /// Remove a specific key from cache to maintain data consistency.
    /// This is critical for cache coherence when data_buf flushes to disk.
    pub fn invalidate(&self, key: RecordKey) -> bool {
        let mut cache_data = self.cache.lock();
        let was_present = cache_data.map.remove(&key).is_some();
        if was_present {
            cache_data.access_times.remove(&key);  // Also remove from access tracking
            cache_data.stats.evictions += 1;
            cache_data.stats.current_size = cache_data.map.len();
            
            #[cfg(not(feature = "linux"))]
            debug!("[ReadCacheSystem] Invalidated block {} for data consistency", key.lba);
            
            true
        } else {
            false
        }
    }

    /// Batch invalidate multiple keys for performance.
    /// Used when data_buf flushes multiple blocks to disk.
    pub fn invalidate_batch(&self, keys: &[RecordKey]) -> usize {
        let mut cache_data = self.cache.lock();
        let mut invalidated_count = 0;
        
        for &key in keys {
            if cache_data.map.remove(&key).is_some() {
                cache_data.access_times.remove(&key);  // Also remove from access tracking
                invalidated_count += 1;
            }
        }
        
        if invalidated_count > 0 {
            cache_data.stats.evictions += invalidated_count;
            cache_data.stats.current_size = cache_data.map.len();
            
            #[cfg(not(feature = "linux"))]
            debug!("[ReadCacheSystem] Batch invalidated {} blocks for data consistency", invalidated_count);
        }
        
        invalidated_count
    }
}

impl CachedBlock {
    /// Create a new cached block with initial access timestamp.
    fn new(data: Box<[u8; BLOCK_SIZE]>, initial_access_time: usize) -> Self {
        Self {
            data,
            access_count: 1,
            last_access_time: initial_access_time,
        }
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
        
        match cache.lookup(key) {
            CacheLookupResult::Hit(cached_block) => {
                assert_eq!(cached_block.data()[0], 42);
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
        assert!(matches!(cache.lookup(first_key), CacheLookupResult::Miss));
        
        // Last key should still be present
        let last_key = RecordKey { lba: READ_CACHE_CAPACITY };
        assert!(matches!(cache.lookup(last_key), CacheLookupResult::Hit(_)));
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
        assert!(matches!(cache.lookup(key), CacheLookupResult::Miss));
        let stats = cache.stats();
        assert_eq!(stats.total_misses(), 1);
        
        // Insert and cause a hit
        let data = Box::new([99u8; BLOCK_SIZE]);
        cache.insert(key, data, CacheInsertHint::Normal)
            .expect("Insert failed");
        
        assert!(matches!(cache.lookup(key), CacheLookupResult::Hit(_)));
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
        
        if let CacheLookupResult::Hit(cached_block) = cache.lookup(key) {
            let mut buf = crate::layers::bio::Buf::alloc(1).unwrap();
            cached_block.copy_to_buf(buf.as_mut()).expect("Copy failed");
            
            assert_eq!(buf.as_slice()[0], 123);
            assert_eq!(buf.as_slice()[100], 231);
        } else {
            panic!("Should hit cache");
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
            if let CacheLookupResult::Hit(cached_block) = cache.lookup(key) {
                assert_eq!(cached_block.data()[0], 42);
            } else {
                panic!("Should hit cache");
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
        assert!(matches!(cache.lookup(key1), CacheLookupResult::Miss));
        assert!(matches!(cache.lookup(key2), CacheLookupResult::Hit(_)));
        
        // Test batch invalidation
        let keys_to_invalidate = [key2, key3];
        let invalidated_count = cache.invalidate_batch(&keys_to_invalidate);
        assert_eq!(invalidated_count, 2);
        assert_eq!(cache.size(), 0);
        assert!(matches!(cache.lookup(key2), CacheLookupResult::Miss));
        assert!(matches!(cache.lookup(key3), CacheLookupResult::Miss));
        
        // Test invalidation of non-existent key
        let was_present = cache.invalidate(RecordKey { lba: 999 });
        assert!(!was_present);
        
        // Verify stats are updated correctly
        let stats = cache.stats();
        assert_eq!(stats.total_evictions(), 3); // 1 + 2 from invalidations
    }
}