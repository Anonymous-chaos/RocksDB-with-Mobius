//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <memory>
#include <string>
#include <shared_mutex>

#include "cache/sharded_cache.h"
#include "port/lang.h"
#include "port/likely.h"
#include "port/malloc.h"
#include "port/port.h"
#include "util/autovector.h"
#include "util/distributed_mutex.h"
#include "util/atomic.h"


namespace ROCKSDB_NAMESPACE {
namespace lru_cache {

// LRU cache implementation. This class is not thread-safe.

// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in a hash table. Some elements
// are also stored on LRU list.
//
// LRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//    In that case the entry is *not* in the LRU list
//    (refs >= 1 && in_cache == true)
// 2. Not referenced externally AND in hash table.
//    In that case the entry is in the LRU list and can be freed.
//    (refs == 0 && in_cache == true)
// 3. Referenced externally AND not in hash table.
//    In that case the entry is not in the LRU list and not in hash table.
//    The entry must be freed if refs becomes 0 in this state.
//    (refs >= 1 && in_cache == false)
// If you call LRUCacheShard::Release enough times on an entry in state 1, it
// will go into state 2. To move from state 1 to state 3, either call
// LRUCacheShard::Erase or LRUCacheShard::Insert with the same key (but
// possibly different value). To move from state 2 to state 1, use
// LRUCacheShard::Lookup.
// While refs > 0, public properties like value and deleter must not change.

struct LRUHandle : public Cache::Handle {
  Cache::ObjectPtr value;
  const Cache::CacheItemHelper* helper;
  LRUHandle* next_hash;
  LRUHandle* next;
  // LRUHandle* prev;
  size_t total_charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  mutable AcqRelAtomic<int32_t> refs{};


  // Mutable flags - access controlled by mutex
  // The m_ and M_ prefixes (and im_ and IM_ later) are to hopefully avoid
  // checking an M_ flag on im_flags or an IM_ flag on m_flags.
  mutable AcqRelAtomic<uint8_t> m_flags;

  // "Immutable" flags - only set in single-threaded context and then
  // can be accessed without mutex
  uint8_t im_flags;

  static constexpr uint8_t M_IN_CACHE_BIT = 0b001;
  static constexpr uint8_t M_HAS_HIT_BIT = 0b010;
  static constexpr uint8_t IM_IS_STANDALONE = 0b100;

  // Beginning of the key (MUST BE THE LAST FIELD IN THIS STRUCT!)
  char key_data[1];

  Slice key() const { return Slice(key_data, key_length); }

  // For HandleImpl concept
  uint32_t GetHash() const { return hash; }

  // Increase the reference count by 1.
  void Ref() { refs.FetchAdd(size_t{1}); }

  // Just reduce the reference count by 1. Return true if it was last reference.
  bool Unref() {
    refs.FetchSub(size_t{1});
    return refs.Load() <= 0;
  }

  // Return true if there are external refs, false otherwise.
  bool HasRefs() const { return refs.Load() > 0; }
  int32_t GetRefs() const { return refs.Load(); }

  bool InCache() const { return m_flags.Load() & M_IN_CACHE_BIT; }
  bool HasHit() const { return m_flags.Load() & M_HAS_HIT_BIT; }
  bool IsStandalone() const { return im_flags & IM_IS_STANDALONE; }

  void SetInCache(bool in_cache) {
    if (in_cache) {
      m_flags.FetchOr(M_IN_CACHE_BIT);
    } else {
      m_flags.FetchAnd(~M_IN_CACHE_BIT);
    }
  }

  void SetHit() { m_flags.FetchOr(M_HAS_HIT_BIT); }

  void ClearHit() { m_flags.FetchAnd(~M_HAS_HIT_BIT); }

  void SetIsStandalone(bool is_standalone) {
    if (is_standalone) {
      im_flags |= IM_IS_STANDALONE;
    } else {
      im_flags &= ~IM_IS_STANDALONE;
    }
  }

  void Free(MemoryAllocator* allocator) {
    assert(refs.Load() == 0);
    assert(helper);
    if (helper->del_cb) {
      helper->del_cb(value, allocator);
    }

    free(this);
  }

  inline size_t CalcuMetaCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    if (metadata_charge_policy != kFullChargeCacheMetadata) {
      return 0;
    } else {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      return malloc_usable_size(
          const_cast<void*>(static_cast<const void*>(this)));
#else
      // This is the size that is used when a new handle is created.
      return sizeof(LRUHandle) - 1 + key_length;
#endif
    }
  }

  // Calculate the memory usage by metadata.
  inline void CalcTotalCharge(
      size_t charge, CacheMetadataChargePolicy metadata_charge_policy) {
    total_charge = charge + CalcuMetaCharge(metadata_charge_policy);
  }

  inline size_t GetCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    size_t meta_charge = CalcuMetaCharge(metadata_charge_policy);
    assert(total_charge >= meta_charge);
    return total_charge - meta_charge;
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class LRUHandleTable {

 public:
  explicit LRUHandleTable(int hash_length_bits, MemoryAllocator* allocator);
  ~LRUHandleTable();

  LRUHandle* Lookup(const Slice& key, uint32_t hash);
  LRUHandle* Insert(LRUHandle* h);
  LRUHandle* Remove(const Slice& key, uint32_t hash);
  LRUHandle* Remove(LRUHandle* h);

  std::shared_mutex& getLock(uint32_t hash);

  template <typename T>
  void ApplyToEntriesRange(T func, size_t index_begin, size_t index_end) {
    for (size_t i = index_begin; i < index_end; i++) {
/************************************************************/
      std::shared_mutex &rwlock_ = rwlocks_[i >> (length_bits_ - locks_length_bits_)];
      std::unique_lock<std::shared_mutex> lock(rwlock_);
/************************************************************/
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        auto n = h->next_hash;
        assert(h->InCache());
        func(h);
        h = n;
      }
    }
  }

  int GetLengthBits() const { return length_bits_; }

  size_t GetOccupancyCount() const { return elems_.Load(); }

  MemoryAllocator* GetAllocator() const { return allocator_; }

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash);

  void Resize();

  // Number of hash bits (upper because lower bits used for sharding)
  // used for table index. Length == 1 << length_bits_
  int length_bits_;

  int locks_length_bits_;

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  std::unique_ptr<LRUHandle*[]> list_;

  std::unique_ptr<std::shared_mutex[]> rwlocks_;

  // Number of elements currently in the table.
  AcqRelAtomic<uint32_t> elems_;

  // From Cache, needed for delete
  MemoryAllocator* const allocator_;
};

// A single shard of sharded cache.
class ALIGN_AS(CACHE_LINE_SIZE) LRUCacheShard final : public CacheShardBase {
 public:
  // NOTE: the eviction_callback ptr is saved, as is it assumed to be kept
  // alive in Cache.
  LRUCacheShard(size_t capacity, bool strict_capacity_limit,
                double high_pri_pool_ratio, double low_pri_pool_ratio,
                bool use_adaptive_mutex,
                CacheMetadataChargePolicy metadata_charge_policy,
                int hash_length_bits, MemoryAllocator* allocator,
                const Cache::EvictionCallback* eviction_callback);

 public:  // Type definitions expected as parameter to ShardedCache
  using HandleImpl = LRUHandle;
  using HashVal = uint32_t;
  using HashCref = uint32_t;

 public:  // Function definitions expected as parameter to ShardedCache
  static inline HashVal ComputeHash(const Slice& key, uint32_t seed) {
    return Lower32of64(GetSliceNPHash64(key, seed));
  }

  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space.
  void SetCapacity(size_t capacity);

  // Set the flag to reject insertion if cache if full.
  void SetStrictCapacityLimit(bool strict_capacity_limit);


  // Like Cache methods, but with an extra "hash" parameter.
  Status Insert(const Slice& key, uint32_t hash, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper, size_t charge,
                LRUHandle** handle, Cache::Priority priority);

  LRUHandle* CreateStandalone(const Slice& key, uint32_t hash,
                              Cache::ObjectPtr obj,
                              const Cache::CacheItemHelper* helper,
                              size_t charge, bool allow_uncharged);

  LRUHandle* Lookup(const Slice& key, uint32_t hash,
                    const Cache::CacheItemHelper* helper,
                    Cache::CreateContext* create_context,
                    Cache::Priority priority, Statistics* stats);

  bool Release(LRUHandle* handle, bool useful, bool erase_if_last_ref);
  bool Ref(LRUHandle* handle);
  void Erase(const Slice& key, uint32_t hash);

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  size_t GetUsage() const;
  size_t GetPinnedUsage() const;
  size_t GetOccupancyCount() const;
  size_t GetTableAddressCount() const;

  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, Cache::ObjectPtr value,
                               size_t charge,
                               const Cache::CacheItemHelper* helper)>& callback,
      size_t average_entries_per_lock, size_t* state);

  void EraseUnRefEntries();

 public:  // other function definitions
  void TEST_GetLRUList(LRUHandle** lru, LRUHandle** lru_low_pri,
                       LRUHandle** lru_bottom_pri);

  // Retrieves number of elements in LRU, for unit test purpose only.
  // Not threadsafe.
  size_t TEST_GetLRUSize();

  // Retrieves high pri pool ratio
  double GetHighPriPoolRatio();

  // Retrieves low pri pool ratio
  double GetLowPriPoolRatio();

  void SetLowPriorityPoolRatio(double low_pri_pool_ratio);

  void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

  void AppendPrintableOptions(std::string& /*str*/) const;

 private:
  friend class LRUCache;
  // Insert an item into the hash table and, if handle is null, insert into
  // the LRU list. Older items are evicted as necessary. Frees `item` on
  // non-OK status.
  Status InsertItem(LRUHandle* item, LRUHandle** handle);

  void LRU_Remove(LRUHandle* e);
  void LRU_Insert(LRUHandle* e);

  // Insert an element to the tail of the cache list.
  void LinkAtTail(AcqRelAtomic<LRUHandle*>& head, AcqRelAtomic<LRUHandle*>& tail, LRUHandle* elemBegin, LRUHandle* elemEnd);

  void clearHitBatch(LRUHandle* begin, LRUHandle* end);

  // Evict a candidate from the cache list and return it.
  // Mobius eviction policy.
  LRUHandle* GetEvictionCandidate0();
  // Mobius with consecutive detection.
  LRUHandle* GetEvictionCandidate1();

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_.

  LRUHandle* CreateHandle(const Slice& key, uint32_t hash,
                          Cache::ObjectPtr value,
                          const Cache::CacheItemHelper* helper, size_t charge);

  void AssignHandle(LRUHandle* e, const Slice& key, uint32_t hash,
                                       Cache::ObjectPtr value,
                                       const Cache::CacheItemHelper* helper,
                                       size_t charge);

  // Initialized before use.
  size_t capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  AcqRelAtomic<LRUHandle*> head_[2];

  AcqRelAtomic<LRUHandle*> tail_[2];

  AcqRelAtomic<uint32_t> size_[2];

  AcqRelAtomic<bool> whichQ_;

  // ------------^^^^^^^^^^^^^-----------
  // Not frequently modified data members
  // ------------------------------------
  //
  // We separate data members that are updated frequently from the ones that
  // are not frequently updated so that they don't share the same cache line
  // which will lead into false cache sharing
  //
  // ------------------------------------
  // Frequently modified data members
  // ------------vvvvvvvvvvvvv-----------
  LRUHandleTable table_;

  // Memory size for entries residing in the cache.
  AcqRelAtomic<size_t> usage_;

  // Memory size for entries residing only in the LRU list.
  AcqRelAtomic<size_t> lru_usage_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable DMutex mutex_;

  // A reference to Cache::eviction_callback_
  const Cache::EvictionCallback& eviction_callback_;
};

class LRUCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache<LRUCacheShard> {
 public:
  explicit LRUCache(const LRUCacheOptions& opts);
  const char* Name() const override { return "LRUCache"; }
  ObjectPtr Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override;

  // Retrieves number of elements in LRU, for unit test purpose only.
  size_t TEST_GetLRUSize();
  // Retrieves high pri pool ratio.
  double GetHighPriPoolRatio();
};

}  // namespace lru_cache

using LRUCache = lru_cache::LRUCache;
using LRUHandle = lru_cache::LRUHandle;
using LRUCacheShard = lru_cache::LRUCacheShard;

}  // namespace ROCKSDB_NAMESPACE
