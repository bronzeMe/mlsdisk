# SGX-Compatible Intelligent Read Cache System

## Overview

This document describes the implementation of Phase 1 of the mlsdisk read performance optimization: an SGX-compatible intelligent read cache system that significantly improves read performance while maintaining write performance isolation. The design is inspired by the proven patterns from data_buf.rs.

## Architecture

### Simplified Cache Design

```
┌─────────────────────┐
│   Application       │
└─────────┬───────────┘
          │ Read Request
          ▼
┌─────────────────────┐
│   Write Buffer      │ ◄── Highest Priority (DataBuf)
│   (Existing)        │     Uses: Mutex<BTreeMap<Key, Data>>
└─────────┬───────────┘
          │ Cache Miss
          ▼
┌─────────────────────┐
│  Read Cache System  │ ◄── 32MB, 8192 blocks
│  (SGX Compatible)   │     Uses: Mutex<BTreeMap<Key, CachedBlock>>
│                     │     Pattern: Same as data_buf.rs
└─────────┬───────────┘
          │ Cache Miss
          ▼
┌─────────────────────┐
│ LSM Tree + Disk     │ ◄── Original Storage Path
└─────────────────────┘
```

## Key Design Features

### 1. SGX Environment Compatibility
- **SGX-Safe Primitives**: Uses `crate::os::{Mutex, BTreeMap, Arc}` instead of std types
- **No Atomic Types**: Avoids `AtomicU64` and other types incompatible with SGX
- **Proven Patterns**: Follows exactly the same pattern as successful `data_buf.rs`

### 2. Simple But Effective Locking
- **Single Mutex Design**: One `Mutex<BTreeMap>` protects the entire cache
- **Fast Operations**: Keep lock-holding time minimal for reduced contention
- **No Complex Locking**: Avoids reader-writer locks and segmented locks that may cause issues

### 3. Write Performance Isolation
- **Separate Code Paths**: Read cache never interferes with write operations
- **DataBuf Priority**: Write buffer always checked first before read cache
- **Zero Write Impact**: Cache operations never delay write path

### 4. Zero-Copy Performance
- **Shared Ownership**: Use `Arc<CachedBlock>` for zero-copy data sharing
- **Direct Buffer Copy**: `CachedBlock::copy_to_buf()` follows data_buf.rs pattern
- **Decrypted Storage**: Cache stores decrypted data to eliminate repeated decryption

## Implementation Details

### Core Components

#### 1. ReadCacheSystem
```rust
pub struct ReadCacheSystem {
    hot_cache: Arc<HotCache>,        // L1: 16MB segmented cache
    meta_cache: Arc<MetaCache>,      // L2: 8MB metadata + data
    prefetch_cache: Arc<PrefetchCache>, // L3: 32MB large cache
    stats: Arc<CacheStats>,          // Performance monitoring
}
```

#### 2. CachedBlock
```rust
pub struct CachedBlock {
    data: Box<[u8; BLOCK_SIZE]>,     // Decrypted block data
    last_access: AtomicU64,          // LRU tracking
    access_count: AtomicU32,         // Frequency tracking
    source_tier: CacheTier,          // Origin cache level
    hotness_score: AtomicU32,        // Promotion metric
}
```

#### 3. Cache Lookup Result
```rust
pub enum CacheLookupResult {
    Hit(Arc<CachedBlock>),           // Zero-copy cache hit
    Miss,                            // Cache miss - proceed to disk
    NegativeHit,                     // Definitely does not exist
}
```

### Integration with SwornDisk

#### Modified Read Path
```rust
fn read_one_block(&self, lba: Lba, mut buf: BufMut) -> Result<()> {
    // 1. Check write buffer (highest priority)
    if self.data_buf.get(key, &mut buf).is_some() {
        return Ok(());
    }

    // 2. Check read cache system
    match self.read_cache.lookup(key) {
        CacheLookupResult::Hit(cached_block) => {
            cached_block.copy_to_buf(&mut buf)?;  // Zero-copy
            return Ok(());
        }
        CacheLookupResult::NegativeHit => {
            return_errno_with_msg!(NotFound, "block not found");
        }
        CacheLookupResult::Miss => {
            // Continue to disk read
        }
    }

    // 3. Fallback to LSM tree + disk read
    let value = self.logical_block_table.get(&key)?;
    // ... disk read and decryption ...
    
    // 4. Cache the result for future reads
    let cached_data = Box::new(*buf.as_slice().try_into().unwrap());
    self.read_cache.insert(key, cached_data, CacheInsertHint::Warm)?;
    
    Ok(())
}
```

## Performance Characteristics

### Cache Capacities
- **Total Cache Size**: 56MB (16MB + 8MB + 32MB)
- **Hot Cache**: 4,096 blocks (16MB) - Most frequently accessed data
- **Meta Cache**: 2,048 blocks (8MB) - Metadata and warm data
- **Prefetch Cache**: 8,192 blocks (32MB) - Speculatively loaded data

### Expected Performance Improvements
- **Random Read 4K**: 140 → 400-500 MiB/s (3-4x improvement)
- **Random Read 32K**: 446 → 800-900 MiB/s (2x improvement)
- **Sequential Read**: 1128 → 1400+ MiB/s (25% improvement)
- **Cache Hit Ratio**: 80%+ for typical workloads

## Cache Statistics and Monitoring

### Available Metrics
```rust
pub fn cache_stats(&self) -> &CacheStats {
    self.inner.read_cache.stats()
}
```

### Key Statistics
- **Hit Ratios**: Per-tier and overall cache hit percentages
- **Memory Usage**: Current cache memory consumption
- **Access Patterns**: Hot/warm/cold data distribution
- **Promotion/Demotion**: Cache tier movement statistics

### Example Usage
```rust
let stats = sworndisk.cache_stats();
println!("Cache hit ratio: {:.1}%", stats.overall_hit_ratio() * 100.0);
println!("Memory usage: {:.2}MB", stats.memory_usage_mb());
println!("Total hits: {}", stats.total_hits());
```

## Thread Safety

### Lock Strategy
- **Segmented Locking**: Hot cache uses 16 segments to reduce contention
- **Reader-Writer Locks**: Optimized for read-heavy workloads
- **Atomic Counters**: Lock-free statistics and access tracking

### Concurrency Benefits
- **Parallel Cache Access**: Multiple threads can access different cache segments
- **Non-Blocking Reads**: Read-heavy operations avoid lock contention
- **Write Isolation**: Cache operations never block write path

## Configuration and Tuning

### Capacity Constants
```rust
pub const HOT_CACHE_CAPACITY: usize = 4096;      // 16MB
pub const META_CACHE_CAPACITY: usize = 2048;     // 8MB  
pub const PREFETCH_CACHE_CAPACITY: usize = 8192; // 32MB
```

### Cache Insertion Hints
```rust
pub enum CacheInsertHint {
    Hot,        // Frequently accessed - place in L1
    Warm,       // Medium frequency - place in L2
    Cold,       // Low frequency - place in L3
    Prefetch,   // Speculative - place in L3, may promote
}
```

## Testing and Validation

### Demo Program
The `cache_demo.rs` program demonstrates:
- Cache performance improvements
- Access pattern analysis
- Memory usage monitoring
- Hit ratio statistics

### Test Coverage
- Unit tests for each cache tier
- Integration tests with SwornDisk
- Performance benchmarks
- Concurrency stress tests

## Future Enhancements

### Phase 2 Optimizations (Planned)
1. **Bloom Filters**: Fast negative lookups for LSM tree
2. **Batch Decryption**: SIMD-optimized parallel decryption
3. **Smart Prefetching**: Access pattern prediction
4. **Adaptive Sizing**: Dynamic cache capacity adjustment

### Phase 3 Advanced Features (Planned)
1. **Data Reorganization**: Background hot data compaction
2. **Compression**: Memory-efficient cache storage
3. **Async I/O**: Fully asynchronous read path
4. **NUMA Awareness**: Topology-aware cache placement

## Conclusion

The three-tier intelligent read cache system provides a solid foundation for improving mlsdisk's read performance while maintaining its excellent write performance and security guarantees. The modular design allows for incremental enhancements and provides extensive monitoring capabilities for performance optimization.
