//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/lru_cache.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include "cache/secondary_cache_adapter.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/lang.h"
#include "util/distributed_mutex.h"


namespace ROCKSDB_NAMESPACE {
namespace lru_cache {


LRUHandleTable::LRUHandleTable(int hash_length_bits,
                               MemoryAllocator* allocator)
    : length_bits_(/* historical starting size*/ hash_length_bits > 17 ? 17:hash_length_bits),
      locks_length_bits_(length_bits_> 10 ? (length_bits_-7) : length_bits_),
      list_(new LRUHandle* [size_t{1} << length_bits_] {}),
      rwlocks_(new std::shared_mutex[size_t{1} << locks_length_bits_]),
      elems_(0),
      allocator_(allocator) {}

LRUHandleTable::~LRUHandleTable() {
  auto alloc = allocator_;
  ApplyToEntriesRange(
      [alloc](LRUHandle* h) {
        // if (!h->HasRefs()) {
          h->Free(alloc);
        // }
      },
      0, size_t{1} << length_bits_);
}

std::shared_mutex& LRUHandleTable::getLock(uint32_t hash) {
  return rwlocks_[hash >> (32 - locks_length_bits_)];
}

LRUHandle* LRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
/************************************************************/
  std::shared_mutex &rwlock_ = getLock(hash);
  std::shared_lock<std::shared_mutex> lock(rwlock_);
/************************************************************/
  LRUHandle* entry = *FindPointer(key, hash);
  if (entry != nullptr) {
    entry->Ref();
    entry->SetHit();
  }
  return entry;
}

LRUHandle* LRUHandleTable::Insert(LRUHandle* h) {
/************************************************************/
  std::shared_mutex &rwlock_ = getLock(h->hash);
  std::unique_lock<std::shared_mutex> lock(rwlock_);
/************************************************************/
  LRUHandle** ptr = FindPointer(h->key(), h->hash);
  LRUHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  h->SetInCache(true);
  h->Ref();
  // If overwritten
  if (old != nullptr) {
    // printf("Overwrite entry: %lx, ref: %u\n", (uint64_t)old, old->GetRefs());
    old->SetInCache(false);
  }
  else{
    elems_.FetchAdd(size_t{1});
  }
  return old;
}

LRUHandle* LRUHandleTable::Remove(const Slice& key, uint32_t hash) {
/************************************************************/
  std::shared_mutex &rwlock_ = getLock(hash);
  std::unique_lock<std::shared_mutex> lock(rwlock_);
/************************************************************/
  LRUHandle** ptr = FindPointer(key, hash);
  LRUHandle* result = *ptr;
  if (result != nullptr) {
    result->SetInCache(false);
    *ptr = result->next_hash;
    elems_.FetchSub(size_t{1});
  }
  return result;
}


LRUHandle** LRUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = &list_[hash >> (32 - length_bits_)];
  while (*ptr != nullptr && (!(*ptr)->InCache() || (*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void LRUHandleTable::Resize() {
}

LRUCacheShard::LRUCacheShard(size_t capacity, bool strict_capacity_limit,
                             double high_pri_pool_ratio,
                             double low_pri_pool_ratio, bool use_adaptive_mutex,
                             CacheMetadataChargePolicy metadata_charge_policy,
                             int hash_length_bits,
                             MemoryAllocator* allocator,
                             const Cache::EvictionCallback* eviction_callback)
    : CacheShardBase(metadata_charge_policy),
      capacity_(0),
      // high_pri_pool_usage_(0),
      // low_pri_pool_usage_(0),
      strict_capacity_limit_(strict_capacity_limit),
      // high_pri_pool_ratio_(high_pri_pool_ratio),
      // high_pri_pool_capacity_(0),
      // low_pri_pool_ratio_(low_pri_pool_ratio),
      // low_pri_pool_capacity_(0),
      head_({AcqRelAtomic<LRUHandle*>(nullptr), AcqRelAtomic<LRUHandle*>(nullptr)}),
      tail_({AcqRelAtomic<LRUHandle*>(nullptr), AcqRelAtomic<LRUHandle*>(nullptr)}),
      size_({AcqRelAtomic<uint32_t>(0), AcqRelAtomic<uint32_t>(0)}),
      whichQ_(0), 
      table_(hash_length_bits, allocator),
      usage_(0),
      lru_usage_(0),
      mutex_(use_adaptive_mutex),
      eviction_callback_(*eviction_callback)
{
  // Make empty circular linked list.
  // lru_.next = &lru_;
  // lru_.prev = &lru_;
  // lru_low_pri_ = &lru_;
  // lru_bottom_pri_ = &lru_;
  SetCapacity(capacity);
}
// This function is not thread safe!!!
void LRUCacheShard::EraseUnRefEntries() {
  MemoryAllocator* alloc = table_.GetAllocator();
  LRUHandle* entry = nullptr;
  // printf("Function - EraseUnRefEntries");

  while(true){
    size_t size0 = size_[0].Load();
    size_t size1 = size_[1].Load();
    if(size0 > 1 || size1 > 1)
      entry = GetEvictionCandidate1();
    else if(size0 == 1)
      entry = head_[0].Load();
    else if(size1 == 1)
      entry = head_[1].Load();
    else
      break;
    assert(entry != nullptr);
    table_.Remove(entry->key(), entry->hash);
    usage_.FetchSub(entry->total_charge);
    entry->Free(alloc);
  }
  head_[0].Store(nullptr);
  head_[1].Store(nullptr);
  tail_[0].Store(nullptr);
  tail_[1].Store(nullptr);
}

// todo
void LRUCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, Cache::ObjectPtr value,
                             size_t charge,
                             const Cache::CacheItemHelper* helper)>& callback,
    size_t average_entries_per_lock, size_t* state) {
  // The state is essentially going to be the starting hash, which works
  // nicely even if we resize between calls because we use upper-most
  // hash bits for table indexes.
  // DMutexLock l(mutex_);
  int length_bits = table_.GetLengthBits();
  size_t length = size_t{1} << length_bits;

  assert(average_entries_per_lock > 0);
  // Assuming we are called with same average_entries_per_lock repeatedly,
  // this simplifies some logic (index_end will not overflow).
  assert(average_entries_per_lock < length || *state == 0);

  size_t index_begin = *state >> (sizeof(size_t) * 8u - length_bits);
  size_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    // Going to end
    index_end = length;
    *state = SIZE_MAX;
  } else {
    *state = index_end << (sizeof(size_t) * 8u - length_bits);
  }

  table_.ApplyToEntriesRange(
      [callback,
       metadata_charge_policy = metadata_charge_policy_](LRUHandle* h) {
        callback(h->key(), h->value, h->GetCharge(metadata_charge_policy),
                 h->helper);
      },
      index_begin, index_end);
}

void LRUCacheShard::TEST_GetLRUList(LRUHandle** lru, LRUHandle** lru_low_pri,
                                    LRUHandle** lru_bottom_pri) {
}

size_t LRUCacheShard::TEST_GetLRUSize() {
  return size_[0].Load() + size_[1].Load();
}

double LRUCacheShard::GetHighPriPoolRatio() {
  return 0;
}

double LRUCacheShard::GetLowPriPoolRatio() {
  return 0;

}

void LRUCacheShard::LRU_Remove(LRUHandle* e) {
}

/********************Created by chaos*******************************/

void LRUCacheShard::LinkAtTail(AcqRelAtomic<LRUHandle*>& head, AcqRelAtomic<LRUHandle*>& tail, LRUHandle* elemBegin, LRUHandle* elemEnd) {
  elemEnd->next = nullptr;
  LRUHandle* oldTail = tail.Load();

  // this is the thread that first makes Tail points to the node
  // other threads must follow this, o.w. oldHead will be nullptr
  while (!tail.CasWeak(oldTail, elemEnd)) {}

  if(oldTail == nullptr){
    head.Store(elemBegin);
  }
  else{
    oldTail->next = elemBegin;
  }
}

void LRUCacheShard::LRU_Insert(LRUHandle* e) {
  assert(e->next == nullptr);
  // printf("Insert entry: %lx\n", (uint64_t)e);
  // fflush(stdout);
  lru_usage_.FetchAdd(e->total_charge);
  bool wQ = whichQ_.Load();
  LinkAtTail(head_[wQ], tail_[wQ], e, e);
  size_[wQ].FetchAdd(1);
  // printf("Finish insert\n");
  // fflush(stdout);
}

/********************Created by chaos*******************************/

void LRUCacheShard::clearHitBatch(LRUHandle* begin, LRUHandle* end) {
  auto *curr = begin;
  while(curr != end && curr != nullptr){
    curr->ClearHit();
    curr = curr->next;
  }
}

LRUHandle* LRUCacheShard::GetEvictionCandidate0() {
  bool wQ = whichQ_.Load();

  auto &activeHead_ = head_[wQ];
  auto &activeTail_ = tail_[wQ];
  auto &activeSize_ = size_[wQ];

  auto &dormantHead_ = head_[!wQ];
  auto &dormantTail_ = tail_[!wQ];
  auto &dormantSize_ = size_[!wQ];

  LRUHandle *curr, *next = nullptr;
  static AcqRelAtomic<uint32_t> evictCount(0);
  

  while(true){
    curr = activeHead_.Load();

    do{
      next = curr->next;
      // When the active Q has only 1 elements, switch the active Q.
      if(next == nullptr && curr == activeHead_.Load()) {
        if(whichQ_.CasWeak(wQ, !wQ)){
          // printf("Switch. Size of Q0: %d, Size of Q1: %d, evictCount: %d\n", size_[0].Load(), size_[1].Load(), evictCount.Load());
        }
        return GetEvictionCandidate0();
      }
    }while(!activeHead_.CasWeak(curr, next));

    activeSize_.FetchSub(1);

    // If the entry is in cache and has a hit, move it to the dormant Q.
    // If the entry is refed, re-add it to the tail of the active Q.
    
    if((curr->InCache() && curr->HasHit()) || curr->HasRefs()){
      curr->ClearHit();
      LinkAtTail(dormantHead_, dormantTail_, curr, curr);
      dormantSize_.FetchAdd(1);
    }
    // Otherwise, remove it as the eviction candidate.
    else{
      evictCount.FetchAdd(1);
      lru_usage_.FetchSub(curr->total_charge);
      return curr;
    }
  }
}

LRUHandle* LRUCacheShard::GetEvictionCandidate1() {
  bool wQ = whichQ_.Load();

  auto &activeHead_ = head_[wQ];
  auto &activeTail_ = tail_[wQ];
  auto &activeSize_ = size_[wQ];

  auto &dormantHead_ = head_[!wQ];
  auto &dormantTail_ = tail_[!wQ];
  auto &dormantSize_ = size_[!wQ];

  LRUHandle *prev, *prevHead, *curr = nullptr;
  int itemCount = 0;
  static AcqRelAtomic<uint32_t> evictCount(0);
  while(true){
    if(curr == nullptr){
      curr = prevHead = activeHead_.Load();
      prev = nullptr;
      itemCount = 0;
    }

    // When the active Q has only 1 elements, switch the active Q.

    if(curr == nullptr || (curr->next == nullptr && prevHead == activeHead_.Load())) {
        if(whichQ_.CasWeak(wQ, !wQ)){
          clearHitBatch(prevHead, curr);
          // printf("Active Q is %d. Size of Q0: %d, Size of Q1: %d, evictCount: %d\n", wQ, size_[0].Load(), size_[1].Load(), evictCount.Load());
        }
        return GetEvictionCandidate1();
    }

    if((curr->InCache() && curr->HasHit()) || curr->HasRefs()){
      prev = curr;
      curr = curr->next;
      itemCount++;
      continue;
    }

    bool headUpdated = activeHead_.CasWeak(prevHead, curr->next);

    if(headUpdated){
      activeSize_.FetchSub(itemCount+1);
      if(prev){
        clearHitBatch(prevHead, curr);
        LinkAtTail(dormantHead_, dormantTail_, prevHead, prev);
        dormantSize_.FetchAdd(itemCount);
      }
      evictCount.FetchAdd(1);
      lru_usage_.FetchSub(curr->total_charge);
      return curr;
    }
    curr = nullptr;
  }
}

void LRUCacheShard::SetCapacity(size_t capacity) {
  capacity_ = capacity;
}

void LRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  strict_capacity_limit_ = strict_capacity_limit;
}


LRUHandle* LRUCacheShard::Lookup(const Slice& key, uint32_t hash,
                                 const Cache::CacheItemHelper* /*helper*/,
                                 Cache::CreateContext* /*create_context*/,
                                 Cache::Priority /*priority*/,
                                 Statistics* /*stats*/) {
  // DMutexLock l(mutex_);
  // printf("Lookup\n");
  // fflush(stdout);

  LRUHandle* e = table_.Lookup(key, hash);
  return e;
}

bool LRUCacheShard::Ref(LRUHandle* e) {
  // To create another reference - entry must be already externally referenced.
  // This funchtion is not thread safe.
  
  assert(e->HasRefs());
  e->Ref();
  return true;
}

void LRUCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
}

void LRUCacheShard::SetLowPriorityPoolRatio(double low_pri_pool_ratio) {
}

bool LRUCacheShard::Release(LRUHandle* e, bool /*useful*/,
                            bool erase_if_last_ref) {

  // printf("Release entry: %lx\n", (uint64_t)e);
  // fflush(stdout);

  if (e == nullptr) {
    return false;
  }
  
  bool no_use = e->Unref();
  bool was_in_cache = e->InCache();
  bool is_standalone = e->IsStandalone();

  if (no_use && is_standalone) {
    assert(usage_ >= e->total_charge);
    usage_.FetchSub(e->total_charge);
    e->Free(table_.GetAllocator());
  }
  // printf("Finish release. Ref: %u\n", e->GetRefs());

  return no_use && was_in_cache;
}

LRUHandle* LRUCacheShard::CreateHandle(const Slice& key, uint32_t hash,
                                       Cache::ObjectPtr value,
                                       const Cache::CacheItemHelper* helper,
                                       size_t charge) {
  assert(helper);
  // value == nullptr is reserved for indicating failure in SecondaryCache
  assert(!(helper->IsSecondaryCacheCompatible() && value == nullptr));

  // static AcqRelAtomic<uint32_t> entryCount(0);
  // entryCount.FetchAdd(1);
  // printf("Create new entry: %u.\n", entryCount.Load());
  
  // Allocate the memory here outside of the mutex.
  // If the cache is full, we'll have to release it.
  // It shouldn't happen very often though.
  LRUHandle* e =
      static_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  AssignHandle(e, key, hash, value, helper, charge);
  return e;
}

void LRUCacheShard::AssignHandle(LRUHandle* e, const Slice& key, uint32_t hash,
                                       Cache::ObjectPtr value,
                                       const Cache::CacheItemHelper* helper,
                                       size_t charge) {
  e->value = value;
  e->m_flags.Store(0);
  e->im_flags = 0;
  e->helper = helper;
  e->key_length = key.size();
  e->hash = hash;
  e->refs.Store(0);
  e->next = nullptr;
  memcpy(e->key_data, key.data(), key.size());
  e->CalcTotalCharge(charge, metadata_charge_policy_);
}

Status LRUCacheShard::Insert(const Slice& key, uint32_t hash,
                             Cache::ObjectPtr value,
                             const Cache::CacheItemHelper* helper,
                             size_t charge, LRUHandle** handle,
                             Cache::Priority priority) {
  Status s = Status::OK();
  LRUHandle* e = nullptr;
  // printf("Start Insert\n");
  // fflush(stdout);
  // If the cache is not full, create a new entry.
  if(usage_.Load() < capacity_){
    e = CreateHandle(key, hash, value, helper, charge);
  }

  // If the cache is full, try to evict an entry.
  else{
    LRUHandle* old = GetEvictionCandidate1();

    // If evict successfully, this entry will be reused for the new value.
    if(old != nullptr){
      if(old->InCache() == true){
        table_.Remove(old->key(), old->hash);
      }

      usage_.FetchSub(old->total_charge);
      // Wait until the entry is not in use.
      while(old->HasRefs()){}
      // Free the value of the old entry.
      if (old->helper->del_cb) {
        // printf("AssignHandle: %lx\n", (uint64_t)e);
        old->helper->del_cb(old->value, table_.GetAllocator());
      }
      AssignHandle(old, key, hash, value, helper, charge);
      e = old;
    }

    // If failed and strict_capacity_limit_ is true, return error. 
    // If failed  but no strict_capacity_limit_, don't insert the entry but still 
    // return ok, as if the entry inserted into cache and get evicted immediately.
    if(old == nullptr){
      if(strict_capacity_limit_ && handle != nullptr)
        s = Status::MemoryLimit("Insert failed due to LRU cache being full.");
      handle = nullptr;
    }
  }

  if(e){
    LRUHandle* old = table_.Insert(e);
    e->ClearHit();
    usage_.FetchAdd(e->total_charge);
    // If the entry is already in cache, remove it.
    if (old != nullptr) {
      s = Status::OkOverwritten();
    }
    if(handle != nullptr) {
      // If caller already holds a ref, no need to take one here.
      *handle = e;
    }
    else{
      e->Unref();
    }
    LRU_Insert(e);
    // printf("Finish insert entry: %lx, ref: %u\n", (uint64_t)e, e->GetRefs());
  }
  return s;
}

LRUHandle* LRUCacheShard::CreateStandalone(const Slice& key, uint32_t hash,
                                           Cache::ObjectPtr value,
                                           const Cache::CacheItemHelper* helper,
                                           size_t charge,
                                           bool allow_uncharged) {
  LRUHandle* e = nullptr;

  // If the cache is not full, create a new entry.
  if(usage_.Load() < capacity_){
    e = CreateHandle(key, hash, value, helper, charge);
  }

  // If the cache is full, try to evict an entry.
  else{
    LRUHandle* old = GetEvictionCandidate1();

    // If evict successfully, this entry will be reused for the new value.
    if(old != nullptr){
      table_.Remove(old->key(), old->hash);
      usage_.FetchSub(old->total_charge);
      AssignHandle(old, key, hash, value, helper, charge);
      e = old;
    }

    // If failed and strict_capacity_limit_ is true
    else if(strict_capacity_limit_){
      if(allow_uncharged) {
        e = CreateHandle(key, hash, value, helper, charge);
        e->total_charge = 0;
      }
      else {
        e = nullptr;
      }
    }

    // If failed and strict_capacity_limit_ is false
    else{
      e = CreateHandle(key, hash, value, helper, charge);
    }
  }

  if(e){
    e->SetIsStandalone(true);
    e->Ref();
    usage_.FetchAdd(e->total_charge);
  }

  return e;
}

void LRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  // printf("Erase\n");
  // fflush(stdout);

  LRUHandle* e = table_.Remove(key, hash);

  // printf("Finish Erase: %lx. \n", (uint64_t)e);
  // fflush(stdout);
}

size_t LRUCacheShard::GetUsage() const {
  // DMutexLock l(mutex_);
  return usage_.Load();
}

size_t LRUCacheShard::GetPinnedUsage() const {
  // DMutexLock l(mutex_);
  assert(usage_.Load() >= lru_usage_.Load());
  return usage_.Load() - lru_usage_.Load();
}

size_t LRUCacheShard::GetOccupancyCount() const {
  // DMutexLock l(mutex_);
  return table_.GetOccupancyCount();
}

size_t LRUCacheShard::GetTableAddressCount() const {
  // DMutexLock l(mutex_);
  return size_t{1} << table_.GetLengthBits();
}

void LRUCacheShard::AppendPrintableOptions(std::string& str) const {
}

LRUCache::LRUCache(const LRUCacheOptions& opts) : ShardedCache(opts) {
  size_t per_shard = GetPerShardCapacity();
  MemoryAllocator* alloc = memory_allocator();
  InitShards([&](LRUCacheShard* cs) {
    new (cs) LRUCacheShard(per_shard, false,
                           opts.high_pri_pool_ratio, opts.low_pri_pool_ratio,
                           opts.use_adaptive_mutex, opts.metadata_charge_policy,
                           /* hash_length_bits */ 32 - opts.num_shard_bits,
                           alloc, &eviction_callback_);
  });
}

Cache::ObjectPtr LRUCache::Value(Handle* handle) {
  auto h = static_cast<const LRUHandle*>(handle);
  return h->value;
}

size_t LRUCache::GetCharge(Handle* handle) const {
  return static_cast<const LRUHandle*>(handle)->GetCharge(
      GetShard(0).metadata_charge_policy_);
}

const Cache::CacheItemHelper* LRUCache::GetCacheItemHelper(
    Handle* handle) const {
  auto h = static_cast<const LRUHandle*>(handle);
  return h->helper;
}

size_t LRUCache::TEST_GetLRUSize() {
  return SumOverShards([](LRUCacheShard& cs) { return cs.TEST_GetLRUSize(); });
}

double LRUCache::GetHighPriPoolRatio() {
  return GetShard(0).GetHighPriPoolRatio();
}

}  // namespace lru_cache

std::shared_ptr<Cache> LRUCacheOptions::MakeSharedCache() const {
  if (num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  // For sanitized options
  LRUCacheOptions opts = *this;
  if (opts.num_shard_bits < 0) {
    opts.num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  std::shared_ptr<Cache> cache = std::make_shared<LRUCache>(opts);
  if (secondary_cache) {
    cache = std::make_shared<CacheWithSecondaryAdapter>(cache, secondary_cache);
  }
  return cache;
}

std::shared_ptr<RowCache> LRUCacheOptions::MakeSharedRowCache() const {
  if (secondary_cache) {
    // Not allowed for a RowCache
    return nullptr;
  }
  // Works while RowCache is an alias for Cache
  return MakeSharedCache();
}
}  // namespace ROCKSDB_NAMESPACE