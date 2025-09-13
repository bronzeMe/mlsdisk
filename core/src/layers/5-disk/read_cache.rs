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
//! - **Write path isolation**: Read cache never interferes with write operations

use super::sworndisk::{RecordKey, RecordValue};
use crate::layers::bio::{BufMut, BLOCK_SIZE};
use crate::os::{BTreeMap, Mutex, Arc};
use crate::prelude::*;

#[cfg(not(feature = "linux"))]
use log::{info, warn};

/// Read cache capacity (32MB = 8192 blocks of 4KB each)
pub(super) const READ_CACHE_CAPACITY: usize = 8192;

/// Optimized read cache system - fixes lock contention issues.
///
/// Uses single-lock design to eliminate lock contention and improve performance.
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
    
    /// Insertion timestamp for eviction decisions
    insert_time: usize,
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
    /// Removed early logging to ensure SGX compatibility during initialization.
    pub fn new() -> Result<Self> {
        // Defer logging until after successful initialization
        Ok(Self {
            cache: Mutex::new(CacheData {
                map: BTreeMap::new(),
                stats: CacheStats::new(),
                insert_counter: 0,
            }),
            capacity: READ_CACHE_CAPACITY,
        })
    }

    /// Optimized cache lookup - fixes dual lock contention and borrowing conflicts.
    ///
    /// Returns cached data if found, using single lock for better performance.
    pub fn lookup(&self, key: RecordKey) -> CacheLookupResult {
        let mut cache_data = self.cache.lock();
        
        if let Some(cached_block) = cache_data.map.get_mut(&key) {
            // Update access counter directly
            if let Some(block) = Arc::get_mut(cached_block) {
                block.access_count += 1;
            }
            
            // Clone the cached block first to avoid borrow conflicts
            let result_block = cached_block.clone();
            
            // Now we can safely update stats
            cache_data.stats.hits += 1;
            CacheLookupResult::Hit(result_block)
        } else {
            cache_data.stats.misses += 1;
            CacheLookupResult::Miss
        }
    }

    /// Optimized cache insertion - fixes dual lock and long-hold issues.
    ///
    /// Uses smart eviction strategy and single lock for better performance.
    pub fn insert(&self, key: RecordKey, data: Box<[u8; BLOCK_SIZE]>, _hint: CacheInsertHint) -> Result<()> {
        let mut cache_data = self.cache.lock();
        
        // Standard LRU eviction: remove one oldest entry if at capacity
        if cache_data.map.len() >= self.capacity {
            if let Some((&oldest_key, _)) = cache_data.map.iter()
                .min_by_key(|(_, block)| block.insert_time) {
                cache_data.map.remove(&oldest_key);
                cache_data.stats.evictions += 1;
            } else {
                // This should never happen if capacity > 0, but return error instead of inserting
                #[cfg(not(feature = "linux"))]
                warn!("Cache capacity control failure - LRU eviction failed");
                return Err(Error::with_msg(OutOfMemory, "Cache LRU eviction failed"));
            }
        }
        
        // Create cached block with current timestamp
        cache_data.insert_counter += 1;
        let cached_block = Arc::new(CachedBlock::new(data, cache_data.insert_counter));
        
        cache_data.map.insert(key, cached_block);
        cache_data.stats.insertions += 1;
        cache_data.stats.current_size = cache_data.map.len();
        
        Ok(())
    }

    /// Get cache statistics - optimized to avoid dual lock.
    pub fn stats(&self) -> CacheStats {
        self.cache.lock().stats.clone()
    }

    /// Clear all cached data - optimized single lock version.
    #[cfg(test)]
    pub fn clear(&self) {
        let mut cache_data = self.cache.lock();
        let cleared_count = cache_data.map.len();
        cache_data.map.clear();
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
}

impl CachedBlock {
    /// Create a new cached block with insertion timestamp.
    fn new(data: Box<[u8; BLOCK_SIZE]>, insert_time: usize) -> Self {
        Self {
            data,
            access_count: 1,
            insert_time,
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

    /// Get insertion time for debugging.
    pub fn insert_time(&self) -> usize {
        self.insert_time
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
}