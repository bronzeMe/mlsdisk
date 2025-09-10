//! Read caching for SwornDisk.
//!
//! This module implements an LRU read cache to improve read performance.
//! The cache stores decrypted data blocks to avoid repeated disk reads and decryption.

use super::sworndisk::RecordKey;
use crate::layers::bio::{BufMut, BLOCK_SIZE};
use crate::os::Mutex;
use crate::prelude::*;

/// An entry in the read cache
struct CacheEntry {
    /// The cached data block
    data: [u8; BLOCK_SIZE],
    /// Last access time (for more advanced replacement policies)
    access_time: u64,
}

/// LRU cache manager for read operations
struct LRU {
    /// Previous pointers for doubly linked list
    prev: Vec<usize>,
    /// Next pointers for doubly linked list
    next: Vec<usize>,
}

impl LRU {
    /// Create a new LRU manager with the given capacity
    fn new(size: usize) -> Self {
        LRU {
            prev: (size - 1..size).chain(0..size - 1).collect(),
            next: (1..size).chain(0..1).collect(),
        }
    }
    
    /// Visit element `id`, move it to head
    fn visit(&mut self, id: usize) {
        if id == 0 || id >= self.prev.len() {
            return;
        }
        self._list_remove(id);
        self._list_insert_head(id);
    }
    
    /// Get a victim at tail
    fn victim(&self) -> usize {
        self.prev[0]
    }
    
    fn _list_remove(&mut self, id: usize) {
        let prev = self.prev[id];
        let next = self.next[id];
        self.prev[next] = prev;
        self.next[prev] = next;
    }
    
    fn _list_insert_head(&mut self, id: usize) {
        let head = self.next[0];
        self.prev[id] = 0;
        self.next[id] = head;
        self.next[0] = id;
        self.prev[head] = id;
    }
}

/// Read cache for SwornDisk
pub struct ReadCache {
    /// Cached data blocks, indexed by position in the LRU list
    cache: Mutex<Vec<Option<(RecordKey, Arc<CacheEntry>)>>>,
    /// LRU manager
    lru: Mutex<LRU>,
    /// Maximum capacity of the cache
    capacity: usize,
    /// Current number of entries in the cache
    size: Mutex<usize>,
}

impl ReadCache {
    /// Create a new read cache with the given capacity
    pub fn new(capacity: usize) -> Self {
        let mut cache = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            cache.push(None);
        }
        
        Self {
            cache: Mutex::new(cache),
            lru: Mutex::new(LRU::new(capacity)),
            capacity,
            size: Mutex::new(0),
        }
    }
    
    /// Get a cached block for the given key and copy it to the buffer
    pub fn get(&self, key: RecordKey, buf: &mut BufMut) -> Option<()> {
        debug_assert_eq!(buf.nblocks(), 1);
        
        let cache = self.cache.lock();
        
        // Search for the key in the cache
        for i in 0..self.capacity {
            if let Some((cached_key, entry)) = &cache[i] {
                if *cached_key == key {
                    // Cache hit, update LRU
                    self.lru.lock().visit(i);
                    
                    // Copy data to buffer
                    buf.as_mut_slice().copy_from_slice(&entry.data);
                    return Some(());
                }
            }
        }
        
        None
    }
    
    /// Check if the key exists in the cache
    pub fn contains(&self, key: RecordKey) -> bool {
        let cache = self.cache.lock();
        
        for i in 0..self.capacity {
            if let Some((cached_key, _)) = &cache[i] {
                if *cached_key == key {
                    return true;
                }
            }
        }
        
        false
    }
    
    /// Put a new block in the cache
    pub fn put(&self, key: RecordKey, data: &[u8]) {
        debug_assert_eq!(data.len(), BLOCK_SIZE);
        
        let mut cache = self.cache.lock();
        let mut lru = self.lru.lock();
        let mut size = self.size.lock();
        
        // Check if the key is already in the cache
        for i in 0..self.capacity {
            if let Some((cached_key, _)) = &cache[i] {
                if *cached_key == key {
                    // Key already exists, update the entry and move to front of LRU
                    let mut entry_data = [0u8; BLOCK_SIZE];
                    entry_data.copy_from_slice(data);
                    
                    let entry = Arc::new(CacheEntry {
                        data: entry_data,
                        access_time: get_current_time(),
                    });
                    
                    cache[i] = Some((key, entry));
                    lru.visit(i);
                    return;
                }
            }
        }
        
        // Key not found, need to add it
        
        // If the cache is full, evict the least recently used entry
        let slot = if *size < self.capacity {
            // Cache not full, use the next empty slot
            *size
        } else {
            // Cache full, use LRU victim
            lru.victim()
        };
        
        // Create new entry
        let mut entry_data = [0u8; BLOCK_SIZE];
        entry_data.copy_from_slice(data);
        
        let entry = Arc::new(CacheEntry {
            data: entry_data,
            access_time: get_current_time(),
        });
        
        // Update cache and LRU
        cache[slot] = Some((key, entry));
        lru.visit(slot);
        
        // Update size if we're adding a new entry
        if *size < self.capacity {
            *size += 1;
        }
    }
    
    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut cache = self.cache.lock();
        let mut lru = self.lru.lock();
        let mut size = self.size.lock();
        
        // Reset all cache entries
        for i in 0..self.capacity {
            cache[i] = None;
        }
        
        // Reset LRU
        *lru = LRU::new(self.capacity);
        *size = 0;
    }
    
    /// Get the current number of entries in the cache
    pub fn size(&self) -> usize {
        *self.size.lock()
    }
    
    /// Get the maximum capacity of the cache
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Get the current time in milliseconds
fn get_current_time() -> u64 {
    #[cfg(feature = "std")]
    {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
    
    #[cfg(not(feature = "std"))]
    {
        // Fallback for non-std environments
        use core::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        COUNTER.fetch_add(1, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::bio::Buf;
    
    #[test]
    fn test_read_cache_basic() {
        let cache = ReadCache::new(10);
        
        // 准备测试数据
        let key = RecordKey { lba: 123 };
        let mut data = [0u8; BLOCK_SIZE];
        for i in 0..BLOCK_SIZE {
            data[i] = (i % 256) as u8;
        }
        
        // 放入缓存
        cache.put(key, &data);
        
        // 检查缓存大小
        assert_eq!(cache.size(), 1);
        
        // 从缓存中获取
        let mut buf = Buf::alloc(1).unwrap();
        let mut buf_mut = buf.as_mut();
        
        let result = cache.get(key, &mut buf_mut);
        assert!(result.is_some());
        
        // 验证数据正确性
        for i in 0..BLOCK_SIZE {
            assert_eq!(buf_mut.as_slice()[i], (i % 256) as u8);
        }
    }
    
    #[test]
    fn test_read_cache_lru() {
        let cache = ReadCache::new(3);
        
        // 放入3个块
        for i in 0..3 {
            let key = RecordKey { lba: i };
            let mut data = [0u8; BLOCK_SIZE];
            data[0] = i as u8;
            cache.put(key, &data);
        }
        
        // 访问第一个块，使其成为最近使用的
        let mut buf = Buf::alloc(1).unwrap();
        let mut buf_mut = buf.as_mut();
        cache.get(RecordKey { lba: 0 }, &mut buf_mut);
        
        // 添加第4个块，应该淘汰第1个块（lba=1）
        let key = RecordKey { lba: 3 };
        let mut data = [0u8; BLOCK_SIZE];
        data[0] = 3;
        cache.put(key, &data);
        
        // 检查lba=1的块是否被淘汰
        let result = cache.get(RecordKey { lba: 1 }, &mut buf_mut);
        assert!(result.is_none());
        
        // 检查其他块是否还在
        assert!(cache.get(RecordKey { lba: 0 }, &mut buf_mut).is_some());
        assert!(cache.get(RecordKey { lba: 2 }, &mut buf_mut).is_some());
        assert!(cache.get(RecordKey { lba: 3 }, &mut buf_mut).is_some());
    }
    
    #[test]
    fn test_read_cache_update() {
        let cache = ReadCache::new(5);
        
        // 放入一个块
        let key = RecordKey { lba: 42 };
        let mut data = [0u8; BLOCK_SIZE];
        data[0] = 123;
        cache.put(key, &data);
        
        // 更新同一个块
        data[0] = 234;
        cache.put(key, &data);
        
        // 验证更新后的值
        let mut buf = Buf::alloc(1).unwrap();
        let mut buf_mut = buf.as_mut();
        
        let result = cache.get(key, &mut buf_mut);
        assert!(result.is_some());
        assert_eq!(buf_mut.as_slice()[0], 234);
    }
    
    #[test]
    fn test_read_cache_clear() {
        let cache = ReadCache::new(5);
        
        // 放入几个块
        for i in 0..5 {
            let key = RecordKey { lba: i };
            let mut data = [0u8; BLOCK_SIZE];
            data[0] = i as u8;
            cache.put(key, &data);
        }
        
        // 清空缓存
        cache.clear();
        
        // 验证缓存已清空
        assert_eq!(cache.size(), 0);
        
        // 验证无法获取之前的块
        let mut buf = Buf::alloc(1).unwrap();
        let mut buf_mut = buf.as_mut();
        
        for i in 0..5 {
            let result = cache.get(RecordKey { lba: i }, &mut buf_mut);
            assert!(result.is_none());
        }
    }
}
