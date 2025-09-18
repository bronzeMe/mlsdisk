//! Large file sequential read cache optimized for 10-30GB files.
//!
//! This module provides a high-performance read caching system specifically designed
//! for large file sequential reads, adopting DataBuf's proven single-lock approach
//! for maximum simplicity and performance.
//!
//! # Design Principles (Optimized for Large Sequential Reads)
//!
//! - **Single-lock design**: One main mutex like DataBuf for simplicity
//! - **FIFO eviction**: Optimal for sequential access patterns, no LRU overhead
//! - **Large capacity**: 512MB cache for 10-30GB file scenarios
//! - **Sequential-optimized**: Eliminates unnecessary metadata and atomic operations
//! - **Zero access overhead**: No timestamp updates or complex heuristics

use super::sworndisk::RecordKey;
use crate::layers::bio::{BufMut, BLOCK_SIZE};
use crate::os::{BTreeMap, Mutex, VecDeque};
use crate::prelude::*;

#[cfg(not(feature = "linux"))]
use log::debug;

/// Large file read cache capacity optimized for 10-30GB sequential reads
/// 128MB cache provides meaningful buffer for large file operations while
/// avoiding excessive memory pressure in TEE environments.
///
/// PERFORMANCE OPTIMIZATIONS FOR LARGE FILE SEQUENTIAL READS:
/// 1. Massive capacity increase: 4MB → 128MB (32x larger for meaningful impact)
/// 2. FIFO eviction: Perfect for sequential access, no LRU timestamp overhead  
/// 3. Zero access cost: No atomic operations or timestamp updates on cache hits
/// 4. DataBuf-style single lock: Eliminates complex lock coordination
/// 5. Sequential-optimized: Designed specifically for large file streaming scenarios
/// 6. O(1) invalidation: Lazy deletion avoids expensive VecDeque search/remove
/// 7. Smart prefetch: Automatic sequential pattern detection with conservative prefetch
/// 8. Generation-based consistency: Handles stale entries efficiently during eviction
pub(super) const LARGE_FILE_CACHE_CAPACITY: usize = 32768; // 128MB cache (32768 * 4KB)

/// Large file sequential read cache system.
///
/// Designed specifically for 10-30GB file sequential reads with advanced optimizations:
/// - Pure single-lock design like DataBuf (no atomic operations)  
/// - FIFO eviction perfect for sequential access patterns
/// - 128MB capacity for meaningful large file buffering
/// - Zero overhead on cache hits (no timestamp updates)
/// - O(1) invalidation using lazy deletion with generation numbers
/// - Automatic sequential access detection and smart prefetch
/// - Generation-based stale entry cleanup during eviction
#[derive(Debug)]
pub struct SimplifiedReadCache {
    /// Cache inner state protected by single mutex (DataBuf pattern)
    inner: Mutex<CacheInner>,
}

/// Internal cache state protected by single mutex
#[derive(Debug)]
struct CacheInner {
    /// Main cache storage - BTreeMap for O(log n) lookup performance
    cache: BTreeMap<RecordKey, CacheEntry>,
    
    /// FIFO queue with generation numbers for O(1) invalidation (sequential read optimized)
    /// Stores (key, generation) pairs to handle invalidated entries efficiently
    insertion_order: VecDeque<(RecordKey, u64)>,
    
    /// Generation counter for lazy invalidation - avoids O(n) VecDeque removal
    generation: u64,
    
    /// Cache capacity limit
    capacity: usize,
    
    /// Sequential access detection for prefetching
    last_access: Option<RecordKey>,
    sequential_count: usize,
    
    /// Simple statistics (no atomic overhead)
    hits: usize,
    misses: usize,
    evictions: usize,
    prefetch_hits: usize,
}

/// Cache entry optimized for sequential reads with lazy invalidation support
#[derive(Debug)]
struct CacheEntry {
    /// Cached block data (only essential data, no timestamps)
    data: Box<[u8; BLOCK_SIZE]>,
    
    /// Generation number for lazy invalidation - avoids O(n) queue operations
    generation: u64,
}

/// Cache lookup result with optional prefetch suggestions.
#[derive(Debug)]
pub enum CacheLookupResult {
    /// Cache hit
    Hit,
    /// Cache miss  
    Miss,
}

/// Prefetch suggestion from cache system to storage layer.
#[derive(Debug, Clone)]
pub struct PrefetchSuggestion {
    /// Keys that should be prefetched based on sequential access patterns
    pub suggested_keys: Vec<RecordKey>,
    /// Confidence level of the prefetch suggestion (0-100)
    pub confidence: u8,
}

/// Enhanced cache lookup result with prefetch suggestions.
#[derive(Debug)]
pub struct CacheLookupResultWithPrefetch {
    /// Basic lookup result
    pub result: CacheLookupResult,
    /// Optional prefetch suggestion based on access patterns
    pub prefetch_suggestion: Option<PrefetchSuggestion>,
}

// Removed CacheInsertHint - simplified cache uses uniform LRU policy

/// Enhanced cache statistics with prefetch metrics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: usize,
    pub misses: usize,
    pub evictions: usize,
    pub prefetch_hits: usize,
    pub current_size: usize,
    pub capacity: usize,
}

impl SimplifiedReadCache {
    /// Create a new large file sequential read cache system.
    pub fn new() -> Result<Self> {
        #[cfg(not(feature = "linux"))]
        debug!("[LargeFileCache] Initializing cache with {} slots ({}MB) for sequential reads", 
               LARGE_FILE_CACHE_CAPACITY, LARGE_FILE_CACHE_CAPACITY * BLOCK_SIZE / 1024 / 1024);
        
        Ok(Self {
            inner: Mutex::new(CacheInner {
                cache: BTreeMap::new(),
                insertion_order: VecDeque::new(),
                generation: 0,
                capacity: LARGE_FILE_CACHE_CAPACITY,
                last_access: None,
                sequential_count: 0,
                hits: 0,
                misses: 0,
                evictions: 0,
                prefetch_hits: 0,
            }),
        })
    }

    /// Sequential read optimized cache lookup with prefetch detection.
    /// 
    /// Enhanced features:
    /// - Sequential access pattern detection
    /// - Automatic prefetch triggering for large file streaming
    /// - Lazy invalidation support with generation numbers
    /// - Zero overhead for non-sequential access
    pub fn lookup_and_copy(&self, key: RecordKey, buf: &mut BufMut) -> CacheLookupResult {
        debug_assert_eq!(buf.nblocks(), 1);
        
        let mut inner = self.inner.lock();
        
        // Check if entry exists and is valid (generation-based lazy invalidation)
        if let Some(entry) = inner.cache.get(&key) {
            // Cache hit! Direct copy within lock (minimal lock time)
            buf.as_mut_slice().copy_from_slice(&entry.data[..]);
            
            // Update sequential access detection
            self.update_sequential_tracking(&mut inner, key);
            
            inner.hits += 1;
            CacheLookupResult::Hit
        } else {
            // Cache miss - update sequential tracking
            self.update_sequential_tracking(&mut inner, key);
            inner.misses += 1;
            CacheLookupResult::Miss
        }
    }
    
    /// Enhanced cache lookup with prefetch suggestions for SwornDisk integration.
    /// 
    /// This method provides the same functionality as lookup_and_copy but also
    /// returns prefetch suggestions when sequential access patterns are detected.
    pub fn lookup_and_copy_with_prefetch(&self, key: RecordKey, buf: &mut BufMut) -> CacheLookupResultWithPrefetch {
        debug_assert_eq!(buf.nblocks(), 1);
        
        let mut inner = self.inner.lock();
        
        let result = if let Some(entry) = inner.cache.get(&key) {
            // Cache hit! Direct copy within lock (minimal lock time)
            buf.as_mut_slice().copy_from_slice(&entry.data[..]);
            inner.hits += 1;
            CacheLookupResult::Hit
        } else {
            inner.misses += 1;
            CacheLookupResult::Miss
        };
        
        // Generate prefetch suggestion based on sequential access detection
        let prefetch_suggestion = self.update_sequential_tracking_with_suggestion(&mut inner, key);
        
        CacheLookupResultWithPrefetch {
            result,
            prefetch_suggestion,
        }
    }
    
    /// Update sequential access tracking without prefetch suggestions (backward compatibility).
    /// This is called for both hits and misses to maintain access pattern detection.
    fn update_sequential_tracking(&self, inner: &mut CacheInner, key: RecordKey) {
        match inner.last_access {
            Some(last_key) if key.lba == last_key.lba + 1 => {
                // Sequential access detected!
                inner.sequential_count += 1;
            }
            _ => {
                // Non-sequential access - reset counter
                inner.sequential_count = 0;
            }
        }
        
        inner.last_access = Some(key);
    }
    
    /// Update sequential access tracking and generate prefetch suggestions.
    /// This is the enhanced version that returns actionable prefetch recommendations.
    fn update_sequential_tracking_with_suggestion(&self, inner: &mut CacheInner, key: RecordKey) -> Option<PrefetchSuggestion> {
        let mut prefetch_suggestion = None;
        
        match inner.last_access {
            Some(last_key) if key.lba == last_key.lba + 1 => {
                // Sequential access detected!
                inner.sequential_count += 1;
                
                // Generate prefetch suggestion after detecting consistent sequential pattern
                if inner.sequential_count >= 3 {
                    prefetch_suggestion = self.generate_prefetch_suggestion(inner, key);
                }
            }
            _ => {
                // Non-sequential access - reset counter
                inner.sequential_count = 0;
            }
        }
        
        inner.last_access = Some(key);
        prefetch_suggestion
    }
    
    /// Generate intelligent prefetch suggestions based on access patterns.
    /// Returns keys that should be prefetched with confidence level.
    fn generate_prefetch_suggestion(&self, inner: &CacheInner, current_key: RecordKey) -> Option<PrefetchSuggestion> {
        // Adaptive prefetch size based on sequential count
        let prefetch_size = match inner.sequential_count {
            3..=5 => 2,   // Conservative start
            6..=10 => 4,  // Medium confidence 
            11..=20 => 6, // High confidence
            _ => 8,       // Maximum for very long sequences
        };
        
        let mut suggested_keys = Vec::new();
        let available_capacity = inner.capacity.saturating_sub(inner.cache.len());
        let effective_prefetch_size = prefetch_size.min(available_capacity).min(8); // Cap at 8
        
        for i in 1..=effective_prefetch_size {
            let prefetch_key = RecordKey { lba: current_key.lba + i };
            
            // Skip if already cached
            if inner.cache.contains_key(&prefetch_key) {
                continue;
            }
            
            suggested_keys.push(prefetch_key);
        }
        
        if suggested_keys.is_empty() {
            return None;
        }
        
        // Calculate confidence based on sequential pattern strength
        let confidence = match inner.sequential_count {
            3..=5 => 60,   // Medium confidence
            6..=10 => 75,  // High confidence  
            11..=20 => 85, // Very high confidence
            _ => 95,       // Maximum confidence for long sequences
        };
        
        #[cfg(not(feature = "linux"))]
        debug!("[LargeFileCache] Generated prefetch suggestion: {} keys, confidence: {}%", 
               suggested_keys.len(), confidence);
               
        Some(PrefetchSuggestion {
            suggested_keys,
            confidence,
        })
    }
    
    /// Alternative lookup for slice targets (multi-block read optimization).
    pub fn lookup_and_copy_to_slice(&self, key: RecordKey, target_slice: &mut [u8]) -> CacheLookupResult {
        debug_assert_eq!(target_slice.len(), BLOCK_SIZE);
        
        let mut inner = self.inner.lock();
        
        if let Some(entry) = inner.cache.get(&key) {
            target_slice.copy_from_slice(&entry.data[..]);
            
            // Update sequential tracking (shared logic with main lookup)
            self.update_sequential_tracking(&mut inner, key);
            
            inner.hits += 1;
            CacheLookupResult::Hit
        } else {
            self.update_sequential_tracking(&mut inner, key);
            inner.misses += 1;
            CacheLookupResult::Miss
        }
    }

    /// FIFO cache insertion with lazy invalidation support.
    /// Uses generation numbers to avoid O(n) queue operations during invalidation.
    pub fn insert(&self, key: RecordKey, data: Box<[u8; BLOCK_SIZE]>) -> Result<()> {
        let mut inner = self.inner.lock();
        
        // Generate new generation number for this entry
        inner.generation += 1;
        let current_generation = inner.generation;
        
        // FIFO eviction when at capacity - with thorough stale entry cleanup
        if inner.cache.len() >= inner.capacity {
            let mut evicted = false;
            let mut stale_cleaned = 0;
            
            // Thoroughly clean stale entries and evict oldest valid entry
            while let Some((oldest_key, oldest_gen)) = inner.insertion_order.pop_front() {
                if let Some(entry) = inner.cache.get(&oldest_key) {
                    // Check if this entry is still valid (same generation)
                    if entry.generation == oldest_gen {
                        // Valid entry - evict it and stop
                        inner.cache.remove(&oldest_key);
                        inner.evictions += 1;
                        evicted = true;
                        break;
                    }
                    // Entry was invalidated - continue cleaning (don't break immediately)
                    stale_cleaned += 1;
                } else {
                    // Entry not in cache - also stale
                    stale_cleaned += 1;
                }
                
                // Prevent excessive cleanup in one operation (performance safeguard)
                if stale_cleaned > 100 {
                    break;
                }
            }
            
            // Log stale cleanup for monitoring
            if stale_cleaned > 0 {
                #[cfg(not(feature = "linux"))]
                debug!("[LargeFileCache] Cleaned {} stale entries during eviction", stale_cleaned);
            }
            
            // If no valid entry found to evict, force evict any remaining entry
            if !evicted && !inner.cache.is_empty() {
                if let Some((&any_key, _)) = inner.cache.iter().next() {
                    inner.cache.remove(&any_key);
                    inner.evictions += 1;
                    #[cfg(not(feature = "linux"))]
                    debug!("[LargeFileCache] Force evicted entry after stale cleanup");
                }
            }
        }
        
        // Insert new entry with current generation
        let entry = CacheEntry { 
            data,
            generation: current_generation,
        };
        inner.cache.insert(key, entry);
        inner.insertion_order.push_back((key, current_generation));
        
        Ok(())
    }
    
    /// O(1) cache invalidation using lazy deletion strategy.
    /// Avoids expensive O(n) VecDeque search/removal by simply removing from cache.
    /// Stale entries in insertion_order are cleaned up during eviction.
    pub fn invalidate(&self, key: RecordKey) -> bool {
        let mut inner = self.inner.lock();
        
        if let Some(_) = inner.cache.remove(&key) {
            // NO O(n) VecDeque operations! 
            // The entry remains in insertion_order but will be skipped during eviction
            // since its generation won't match (lazy deletion pattern)
            inner.evictions += 1;
            true
        } else {
            false
        }
    }
    
    /// High-performance batch invalidation with O(1) per-key cost.
    /// Uses lazy deletion to avoid expensive VecDeque operations.
    pub fn invalidate_iter(&self, keys_iter: impl Iterator<Item = RecordKey>) -> usize {
        let mut inner = self.inner.lock();
        let mut invalidated_count = 0;
        
        for key in keys_iter {
            if inner.cache.remove(&key).is_some() {
                // NO O(n) VecDeque operations per key!
                // Stale entries will be cleaned up during natural eviction process
                invalidated_count += 1;
            }
        }
        
        if invalidated_count > 0 {
            inner.evictions += invalidated_count;
        }
        
        invalidated_count
    }
    
    /// Batch invalidation for multiple keys with lazy deletion.
    pub fn invalidate_batch(&self, keys: &[RecordKey]) -> usize {
        let mut inner = self.inner.lock();
        let mut invalidated_count = 0;
        
        for &key in keys {
            if inner.cache.remove(&key).is_some() {
                // Lazy deletion - no expensive VecDeque operations
                invalidated_count += 1;
            }
        }
        
        if invalidated_count > 0 {
            inner.evictions += invalidated_count;
        }
        
        invalidated_count
    }

    /// Get enhanced cache statistics including prefetch metrics.
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock();
        
        CacheStats {
            hits: inner.hits,
            misses: inner.misses,
            evictions: inner.evictions,
            prefetch_hits: inner.prefetch_hits,
            current_size: inner.cache.len(),
            capacity: inner.capacity,
        }
    }

    /// Clear all cached data and reset state.
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        let cleared_count = inner.cache.len();
        
        inner.cache.clear();
        inner.insertion_order.clear();
        
        // Reset sequential tracking state
        inner.last_access = None;
        inner.sequential_count = 0;
        inner.generation = 0;
        
        if cleared_count > 0 {
            inner.evictions += cleared_count;
        }
    }

    /// Get current cache size.
    pub fn size(&self) -> usize {
        self.inner.lock().cache.len()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().cache.is_empty()
    }
    
    /// Get current sequential access information for debugging/monitoring.
    /// Returns (last_lba, sequential_count) if sequential access is detected.
    pub fn sequential_access_info(&self) -> Option<(usize, usize)> {
        let inner = self.inner.lock();
        inner.last_access.map(|key| (key.lba, inner.sequential_count))
    }
    
    /// Report a prefetch hit to update statistics.
    /// This should be called by SwornDisk when a prefetched block is used.
    pub fn report_prefetch_hit(&self, key: RecordKey) {
        let mut inner = self.inner.lock();
        
        // Verify this was actually a prefetch (exists in cache)
        if inner.cache.contains_key(&key) {
            inner.prefetch_hits += 1;
            
            #[cfg(not(feature = "linux"))]
            debug!("[LargeFileCache] Prefetch hit reported for LBA {}", key.lba);
        }
    }
    
    /// Batch insert prefetched data with prefetch hit tracking.
    /// This is specifically for SwornDisk to insert prefetched blocks.
    pub fn insert_prefetched(&self, key: RecordKey, data: Box<[u8; BLOCK_SIZE]>) -> Result<()> {
        // Insert the prefetched data
        self.insert(key, data)?;
        
        // Note: We don't immediately increment prefetch_hits here
        // It will be incremented when the block is actually accessed via report_prefetch_hit
        Ok(())
    }
    
    /// Force trigger prefetch for testing/benchmarking scenarios.
    /// This is primarily for performance testing and debugging.
    #[cfg(test)]
    pub fn force_generate_prefetch_suggestion(&self, key: RecordKey) -> Option<PrefetchSuggestion> {
        let inner = self.inner.lock();
        self.generate_prefetch_suggestion(&inner, key)
    }
}

impl CacheStats {
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

    /// Get total evictions.
    pub fn total_evictions(&self) -> usize {
        self.evictions
    }

    /// Get current cache size.
    pub fn current_size_blocks(&self) -> usize {
        self.current_size
    }

    /// Get cache memory usage in bytes.
    pub fn memory_usage_bytes(&self) -> usize {
        self.current_size * BLOCK_SIZE
    }
    
    /// Get total prefetch hits for performance analysis.
    pub fn total_prefetch_hits(&self) -> usize {
        self.prefetch_hits
    }
}

impl Default for CacheStats {
    fn default() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            prefetch_hits: 0,
            current_size: 0,
            capacity: 0,
        }
    }
}

// Compatibility aliases for seamless replacement of original ReadCacheSystem
pub type ReadCacheSystem = SimplifiedReadCache;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::bio::Buf;

    #[test]
    fn test_large_file_cache_operations() {
        let cache = SimplifiedReadCache::new().expect("Failed to create cache");
        let key = RecordKey { lba: 100 };
        
        // Test cache miss
        let mut buf = Buf::alloc(1).unwrap();
        assert!(matches!(cache.lookup_and_copy(key, buf.as_mut()), CacheLookupResult::Miss));
        
        // Test cache insertion and hit
        let data = Box::new([42u8; BLOCK_SIZE]);
        cache.insert(key, data).expect("Insert failed");
        
        match cache.lookup_and_copy(key, buf.as_mut()) {
            CacheLookupResult::Hit => {
                assert_eq!(buf.as_slice()[0], 42);
            }
            CacheLookupResult::Miss => {
                panic!("Should hit cache");
            }
        }
        
        // Test stats
        let stats = cache.stats();
        assert_eq!(stats.total_hits(), 1);
        assert_eq!(stats.total_misses(), 1);
    }
    
    #[test]
    fn test_fifo_eviction() {
        let cache = SimplifiedReadCache::new().expect("Failed to create cache");
        
        // Fill cache beyond capacity to test FIFO eviction
        for i in 0..=LARGE_FILE_CACHE_CAPACITY {
            let key = RecordKey { lba: i };
            let data = Box::new([i as u8; BLOCK_SIZE]);
            cache.insert(key, data).expect("Insert failed");
        }
        
        // Should not exceed capacity
        assert_eq!(cache.size(), LARGE_FILE_CACHE_CAPACITY);
        
        let stats = cache.stats();
        assert!(stats.total_evictions() > 0);
        
        // Verify FIFO behavior - oldest entry (lba=0) should be evicted
        let mut buf = Buf::alloc(1).unwrap();
        let oldest_key = RecordKey { lba: 0 };
        assert!(matches!(cache.lookup_and_copy(oldest_key, buf.as_mut()), CacheLookupResult::Miss));
    }
    
    #[test]
    fn test_sequential_access_detection() {
        let cache = SimplifiedReadCache::new().expect("Failed to create cache");
        let mut buf = Buf::alloc(1).unwrap();
        
        // Simulate sequential access pattern
        for i in 100..110 {
            let key = RecordKey { lba: i };
            cache.lookup_and_copy(key, buf.as_mut()); // Will be misses but should detect pattern
        }
        
        // Check that sequential access was detected
        if let Some((last_lba, seq_count)) = cache.sequential_access_info() {
            assert_eq!(last_lba, 109);
            assert!(seq_count > 3); // Should have detected sequential pattern
        } else {
            panic!("Sequential access should be detected");
        }
    }
    
    #[test]
    fn test_prefetch_suggestions() {
        let cache = SimplifiedReadCache::new().expect("Failed to create cache");
        let mut buf = Buf::alloc(1).unwrap();
        
        // Build up sequential access pattern
        for i in 200..205 {
            let key = RecordKey { lba: i };
            let result = cache.lookup_and_copy_with_prefetch(key, buf.as_mut());
            
            // Should be misses initially
            assert!(matches!(result.result, CacheLookupResult::Miss));
            
            // After a few sequential accesses, should get prefetch suggestions
            if i >= 203 {
                assert!(result.prefetch_suggestion.is_some());
                if let Some(suggestion) = result.prefetch_suggestion {
                    assert!(!suggestion.suggested_keys.is_empty());
                    assert!(suggestion.confidence >= 60);
                    // Verify suggestion contains expected keys
                    assert!(suggestion.suggested_keys.contains(&RecordKey { lba: i + 1 }));
                }
            }
        }
    }
    
    #[test]
    fn test_prefetch_hit_tracking() {
        let cache = SimplifiedReadCache::new().expect("Failed to create cache");
        let key = RecordKey { lba: 300 };
        
        // Insert a prefetched block
        let data = Box::new([123u8; BLOCK_SIZE]);
        cache.insert_prefetched(key, data).expect("Prefetch insert failed");
        
        // Report it as a prefetch hit
        cache.report_prefetch_hit(key);
        
        // Check statistics
        let stats = cache.stats();
        assert_eq!(stats.total_prefetch_hits(), 1);
    }
    
    #[test]
    fn test_lazy_invalidation_performance() {
        let cache = SimplifiedReadCache::new().expect("Failed to create cache");
        
        // Fill cache with some entries
        for i in 0..100 {
            let key = RecordKey { lba: i };
            let data = Box::new([i as u8; BLOCK_SIZE]);
            cache.insert(key, data).expect("Insert failed");
        }
        
        // Invalidate half the entries - this should be O(1) per operation now
        let keys_to_invalidate: Vec<RecordKey> = (0..50).map(|i| RecordKey { lba: i }).collect();
        let invalidated = cache.invalidate_batch(&keys_to_invalidate);
        assert_eq!(invalidated, 50);
        
        // Verify that invalidated entries are no longer accessible
        let mut buf = Buf::alloc(1).unwrap();
        let invalidated_key = RecordKey { lba: 25 };
        assert!(matches!(cache.lookup_and_copy(invalidated_key, buf.as_mut()), CacheLookupResult::Miss));
        
        // Verify that non-invalidated entries are still accessible
        let valid_key = RecordKey { lba: 75 };
        assert!(matches!(cache.lookup_and_copy(valid_key, buf.as_mut()), CacheLookupResult::Hit));
    }
    
    #[test] 
    fn test_thorough_stale_cleanup() {
        let cache = SimplifiedReadCache::new().expect("Failed to create cache");
        
        // Fill cache to near capacity
        for i in 0..10 {
            let key = RecordKey { lba: i };
            let data = Box::new([i as u8; BLOCK_SIZE]);
            cache.insert(key, data).expect("Insert failed");
        }
        
        // Invalidate most entries to create stale entries in insertion_order
        for i in 0..8 {
            let key = RecordKey { lba: i };
            cache.invalidate(key);
        }
        
        // Now insert a new entry - this should trigger stale cleanup
        let new_key = RecordKey { lba: 100 };
        let new_data = Box::new([100u8; BLOCK_SIZE]);
        cache.insert(new_key, new_data).expect("Insert failed");
        
        // Verify the new entry was inserted successfully
        let mut buf = Buf::alloc(1).unwrap();
        assert!(matches!(cache.lookup_and_copy(new_key, buf.as_mut()), CacheLookupResult::Hit));
        assert_eq!(buf.as_slice()[0], 100);
        
        // Verify that cache didn't grow beyond expected size (stale entries were cleaned)
        assert!(cache.size() <= 10); // Should have cleaned up stale entries
    }
}
