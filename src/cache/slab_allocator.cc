#include "cache/slab_allocator.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <mutex>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/disk_lifetime_clock.h"
#include "cache/disk_lifetime_math.h"
#include "cache/disk_lifetime_state.h"
#include "common/config_dump.h"
#include "common/kv_types.h"

namespace dfkv {
namespace {

uint64_t AlignUp(uint64_t x, uint64_t a) {
  if (a <= 1) return x;
  if (x > std::numeric_limits<uint64_t>::max() - (a - 1)) return 0;
  return ((x + (a - 1)) / a) * a;
}

}  // namespace

uint64_t SlabAllocator::FlatKeyIndex::Hash(const BlockKey& key) {
  auto mix = [](uint64_t value) {
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
  };
  uint64_t hash = mix(key.digest_hi + 0x9e3779b97f4a7c15ULL);
  hash ^= mix(key.digest_lo + 0x3c6ef372fe94f82aULL);
  hash ^= mix(key.tenant_hash + 0xdaa66d2c7ddef743ULL);
  return mix(hash);
}

uint32_t* SlabAllocator::FlatKeyIndex::Find(const BlockKey& key) {
  if (entries_.empty()) return nullptr;
  const size_t mask = entries_.size() - 1;
  size_t bucket = static_cast<size_t>(Hash(key)) & mask;
  for (size_t probed = 0; probed < entries_.size(); ++probed) {
    Entry& entry = entries_[bucket];
    if (entry.state == 0) return nullptr;
    if (entry.state == 1 && entry.key == key) return &entry.value;
    bucket = (bucket + 1) & mask;
  }
  return nullptr;
}

const uint32_t* SlabAllocator::FlatKeyIndex::Find(
    const BlockKey& key) const {
  return const_cast<FlatKeyIndex*>(this)->Find(key);
}

void SlabAllocator::FlatKeyIndex::Rehash(size_t requested_capacity) {
  size_t capacity = 16;
  while (capacity < requested_capacity) capacity *= 2;
  std::vector<Entry> replacement(capacity);
  const size_t mask = capacity - 1;
  for (const Entry& old : entries_) {
    if (old.state != 1) continue;
    size_t bucket = static_cast<size_t>(Hash(old.key)) & mask;
    while (replacement[bucket].state == 1) bucket = (bucket + 1) & mask;
    replacement[bucket] = old;
  }
  entries_.swap(replacement);
  tombstones_ = 0;
  ++table_allocations_;
  ++rehashes_;
}

void SlabAllocator::FlatKeyIndex::Reserve(size_t expected_size) {
  size_t capacity = entries_.empty() ? 16 : entries_.size();
  while (expected_size > (capacity * 7) / 10) capacity *= 2;
  if (entries_.empty() || capacity != entries_.size() ||
      (tombstones_ != 0 &&
       (expected_size + tombstones_ > (capacity * 7) / 10 ||
        tombstones_ > capacity / 4))) {
    Rehash(capacity);
  }
}

bool SlabAllocator::FlatKeyIndex::Insert(const BlockKey& key,
                                         uint32_t value) {
  if (uint32_t* existing = Find(key)) {
    *existing = value;
    return false;
  }
  Reserve(size_ + 1);
  const size_t mask = entries_.size() - 1;
  size_t bucket = static_cast<size_t>(Hash(key)) & mask;
  size_t tombstone = entries_.size();
  for (size_t probed = 0; probed < entries_.size(); ++probed) {
    Entry& entry = entries_[bucket];
    if (entry.state == 0) {
      const size_t target = tombstone == entries_.size() ? bucket : tombstone;
      Entry& inserted = entries_[target];
      if (inserted.state == 2) --tombstones_;
      inserted.key = key;
      inserted.value = value;
      inserted.state = 1;
      ++size_;
      return true;
    }
    if (entry.state == 2 && tombstone == entries_.size()) tombstone = bucket;
    bucket = (bucket + 1) & mask;
  }
  return false;
}

bool SlabAllocator::FlatKeyIndex::Erase(const BlockKey& key) {
  if (entries_.empty()) return false;
  const size_t mask = entries_.size() - 1;
  size_t bucket = static_cast<size_t>(Hash(key)) & mask;
  for (size_t probed = 0; probed < entries_.size(); ++probed) {
    Entry& entry = entries_[bucket];
    if (entry.state == 0) return false;
    if (entry.state == 1 && entry.key == key) {
      entry.state = 2;
      entry.value = kNoSlot;
      --size_;
      ++tombstones_;
      return true;
    }
    bucket = (bucket + 1) & mask;
  }
  return false;
}

SlabAllocator::IndexStats SlabAllocator::FlatKeyIndex::stats() const {
  return IndexStats{size_, entries_.size(), tombstones_, table_allocations_,
                    rehashes_};
}

size_t SlabAllocator::FlatKeyIndex::BucketForTest(const BlockKey& key) const {
  return entries_.empty()
             ? 0
             : static_cast<size_t>(Hash(key)) & (entries_.size() - 1);
}

SlabAllocator::SlabAllocator(Options opt) : opt_(std::move(opt)) {
  // Lifetime metadata always belongs to a committed generation; never allow
  // state tracking without the matching physical-resident identity storage.
  if (opt_.track_lifetime_state) opt_.track_resident_identity = true;
  if (opt_.track_lifetime_state) {
    assert(opt_.lifetime_max_reads >= 1 && opt_.lifetime_max_reads <= 63);
  }
  assert(opt_.lifetime_bucket_count == 0 ||
         (opt_.track_lifetime_state && opt_.lifetime_bucket_sec > 0 &&
          opt_.lifetime_bucket_count <= 64));
  assert((opt_.lifetime_donor_config == nullptr) ==
         (opt_.lifetime_donor_clock == nullptr));
  assert(
      opt_.lifetime_donor_config == nullptr ||
      (opt_.lifetime_bucket_count != 0 &&
       opt_.lifetime_donor_config->bucket_count == opt_.lifetime_bucket_count &&
       opt_.lifetime_donor_config->bucket_sec == opt_.lifetime_bucket_sec));
  if (opt_.align == 0) opt_.align = 1;
  if (opt_.num_extents == 0) opt_.num_extents = 1;
  if (opt_.max_waste < 0) opt_.max_waste = 0;
  extents_.assign(opt_.num_extents, ExtentMeta{});
  if (opt_.lifetime_bucket_count != 0) {
    assert(opt_.num_extents <= SIZE_MAX / opt_.lifetime_bucket_count);
    lifetime_buckets_.resize(static_cast<size_t>(opt_.num_extents) *
                             opt_.lifetime_bucket_count);
    lifetime_bucket_summary_.resize(opt_.lifetime_bucket_count);
  }
  unbound_ = opt_.num_extents;

  // Establish the complete canonical class lattice once. Class indices and
  // counts are thereafter immutable and independent of request/restore order.
  const uint64_t uint32_aligned =
      (static_cast<uint64_t>(UINT32_MAX) / opt_.align) * opt_.align;
  const uint64_t max_aligned = std::min(
      (opt_.extent_bytes / opt_.align) * static_cast<uint64_t>(opt_.align),
      uint32_aligned);
  for (uint64_t slot_size = opt_.align; slot_size <= max_aligned;) {
    Class cls;
    cls.slot_size = static_cast<uint32_t>(slot_size);
    cls.slots_per_extent =
        static_cast<uint32_t>(opt_.extent_bytes / slot_size);
    classes_.push_back(std::move(cls));
    if (slot_size == max_aligned) break;
    const uint64_t next = CanonicalSlotSize(slot_size + opt_.align);
    if (next <= slot_size) break;
    slot_size = next;
  }

  if (opt_.track_lifetime_state) {
    lifetime_versions_.resize(classes_.size());
    lifetime_queues_.resize((classes_.size() + 1) *
                            (opt_.lifetime_max_reads + 1));
  }

  if (const char* w = std::getenv("DFKV_SLAB_COLD_STEAL_WINDOW")) {
    char* end = nullptr;
    const unsigned long long v = std::strtoull(w, &end, 10);
    if (end != w) {
      if (v == 0) cold_steal_enabled_ = false;
      else cold_window_ = static_cast<uint64_t>(v);
    }
  }
  config_dump::RecordResolved(
      "DFKV_SLAB_COLD_STEAL_WINDOW",
      !cold_steal_enabled_ ? std::string("disabled")
          : (cold_window_ ? std::to_string(cold_window_) : std::string("auto")));
}

void SlabAllocator::PushFreeLocked(Class& cls, uint32_t extent,
                                   uint32_t slot) {
  std::vector<uint32_t>& slots = cls.free_by_ext[extent];
  if (slots.empty()) {
    const size_t capacity = cls.ext_rr.capacity();
    cls.ext_rr.push_back(extent);
    // Account each completed growth before the next allocation can fail.
    class_rotation_bytes_ +=
        (cls.ext_rr.capacity() - capacity) * sizeof(uint32_t);
  }
  const size_t capacity = slots.capacity();
  slots.push_back(slot);
  class_free_slot_bytes_ += (slots.capacity() - capacity) * sizeof(uint32_t);
  ++cls.free_count;
}

bool SlabAllocator::PopFreeLocked(Class& cls, Slot* out) {
  if (cls.ext_rr.empty()) return false;
  if (cls.rr_next >= cls.ext_rr.size()) cls.rr_next = 0;
  const uint32_t extent = cls.ext_rr[cls.rr_next];
  auto it = cls.free_by_ext.find(extent);
  std::vector<uint32_t>& slots = it->second;
  const uint32_t slot = slots.back();
  slots.pop_back();
  --cls.free_count;
  if (slots.empty()) {
    cls.ext_rr[cls.rr_next] = cls.ext_rr.back();
    cls.ext_rr.pop_back();
  } else {
    ++cls.rr_next;
  }
  if (out) *out = Slot{extent, slot};
  return true;
}

void SlabAllocator::DropFromRotationLocked(Class& cls, uint32_t extent) {
  for (size_t i = 0; i < cls.ext_rr.size(); ++i) {
    if (cls.ext_rr[i] != extent) continue;
    cls.ext_rr[i] = cls.ext_rr.back();
    cls.ext_rr.pop_back();
    if (cls.rr_next > i) --cls.rr_next;
    if (cls.rr_next >= cls.ext_rr.size() && !cls.ext_rr.empty()) cls.rr_next = 0;
    return;
  }
}

bool SlabAllocator::TakeFreeSlotLocked(Class& cls, uint32_t extent,
                                       uint32_t slot) {
  auto it = cls.free_by_ext.find(extent);
  if (it == cls.free_by_ext.end()) return false;
  std::vector<uint32_t>& slots = it->second;
  auto found = std::find(slots.begin(), slots.end(), slot);
  if (found == slots.end()) return false;
  *found = slots.back();
  slots.pop_back();
  --cls.free_count;
  if (slots.empty()) {
    DropFromRotationLocked(cls, extent);
  }
  return true;
}

void SlabAllocator::DropExtentFreeLocked(Class& cls, uint32_t extent) {
  const std::unordered_map<uint32_t, std::vector<uint32_t>>::iterator it =
      cls.free_by_ext.find(extent);
  if (it == cls.free_by_ext.end()) return;
  cls.free_count -= it->second.size();
  // Even an empty free list retains capacity until its map entry is erased.
  class_free_slot_bytes_ -= it->second.capacity() * sizeof(uint32_t);
  cls.free_by_ext.erase(it);
  DropFromRotationLocked(cls, extent);
}

uint32_t SlabAllocator::CanonicalSlotSize(size_t aligned_len) const {
  // Build a fixed geometric lattice from alignment and extent geometry. The
  // next bucket is the largest aligned slot for which the smallest request not
  // served by the preceding bucket still satisfies max_waste. Consequently a
  // request's class never depends on which other request arrived first.
  const uint64_t uint32_aligned =
      (static_cast<uint64_t>(UINT32_MAX) / opt_.align) * opt_.align;
  const uint64_t max_aligned = std::min(
      (opt_.extent_bytes / opt_.align) * static_cast<uint64_t>(opt_.align),
      uint32_aligned);
  uint64_t current = opt_.align;
  const double waste = std::min(opt_.max_waste, 0.999999);
  while (current < aligned_len) {
    const uint64_t minimum = current + opt_.align;
    uint64_t candidate = minimum;
    if (waste > 0) {
      const double raw = static_cast<double>(minimum) / (1.0 - waste);
      const uint64_t bounded = static_cast<uint64_t>(raw);
      candidate = (bounded / opt_.align) * static_cast<uint64_t>(opt_.align);
      if (candidate < minimum) candidate = minimum;
    }
    if (candidate > max_aligned) candidate = max_aligned;
    if (candidate <= current) return static_cast<uint32_t>(aligned_len);
    current = candidate;
  }
  return static_cast<uint32_t>(current);
}

size_t SlabAllocator::ClassForLen(size_t aligned_len) {
  return ClassForExactSize(CanonicalSlotSize(aligned_len));
}

size_t SlabAllocator::ClassForExactSize(uint32_t slot_size) {
  for (size_t i = 0; i < classes_.size(); ++i) {
    if (classes_[i].slot_size == slot_size) return i;
  }
  return classes_.size();
}

void SlabAllocator::CommitResidentGeneration(
    const BlockKey& key, uint64_t generation,
    const DiskLifetimeState* lifetime_state, const DiskLifetimeConfig* config,
    uint32_t now) {
  const std::lock_guard<std::mutex> lock(mu_);
  const uint32_t* index = index_.Find(key);
  assert(opt_.track_resident_identity && index != nullptr && generation != 0);
  assert(slots_[*index].live && !slots_[*index].remove_pending);
  assert(resident_generations_[*index] == 0);
  assert(lifetime_state == nullptr || opt_.track_lifetime_state);
  assert(!opt_.track_lifetime_state ||
         !resident_lifetime_states_[*index].has_value());
  resident_generations_[*index] = generation;
  if (opt_.track_lifetime_state && lifetime_state != nullptr) {
    resident_lifetime_states_[*index] = *lifetime_state;
    if (lifetime_state->history_unknown) {
      Class& cls = classes_[slots_[*index].ref.cls];
      ++cls.recovered_residents;
      if (lifetime_state->actual_reads == 0) ++cls.recovered_unread_residents;
    }
    LinkLifetimeLocked(*index);
    if (opt_.lifetime_bucket_count != 0) {
      assert(config != nullptr);
      RegisterLifetimeBucketLocked(*index, *config, now);
    }
  }
}

SlabAllocator::LifetimeReadResult SlabAllocator::ApplyLifetimeRead(
    const BlockKey& key, uint64_t generation, const DiskLifetimeConfig& config,
    const DiskLifetimeClock& clock) {
  const std::lock_guard<std::mutex> lock(mu_);
  const uint32_t* index = index_.Find(key);
  if (!opt_.track_resident_identity || index == nullptr || generation == 0 ||
      !slots_[*index].live || slots_[*index].remove_pending)
    return LifetimeReadResult::kResidentChanged;
  if (resident_generations_[*index] != generation)
    return LifetimeReadResult::kGenerationMismatch;
  uint32_t applied_at = 0;
  // Hard exhaustion takes priority over a deliberately uninitialized state
  // left by an already-admitted NEW commit beyond the representable clock.
  if (!clock.TryDrainNow(&applied_at))
    return LifetimeReadResult::kClockHardExhausted;
  if (!opt_.track_lifetime_state) return LifetimeReadResult::kStateUnavailable;
  std::optional<DiskLifetimeState>& resident_state =
      resident_lifetime_states_[*index];
  if (!resident_state.has_value()) return LifetimeReadResult::kStateUnavailable;
  DiskLifetimeState next_state = *resident_state;
  if (!ApplyDiskLifetimeRead(config, applied_at, &next_state))
    return LifetimeReadResult::kStateUnavailable;
  // Virtual recovery reads never leave the unread population. Only the first
  // accepted real read does; rejected updates keep both population ledgers.
  if (resident_state->history_unknown && resident_state->actual_reads == 0 &&
      next_state.actual_reads != 0) {
    Class& cls = classes_[slots_[*index].ref.cls];
    assert(cls.recovered_unread_residents != 0);
    --cls.recovered_unread_residents;
  }
  // Even a saturated read changes anchor and must move BOTH indexes. Compute
  // the new state first so a rejected update leaves state and links untouched.
  UnregisterLifetimeBucketLocked(*index);
  UnlinkLifetimeLocked(*index);
  resident_state = next_state;
  LinkLifetimeLocked(*index);
  RegisterLifetimeBucketLocked(*index, config, applied_at);
  return LifetimeReadResult::kApplied;
}

uint64_t SlabAllocator::ResidentGeneration(const BlockKey& key) const {
  if (!opt_.track_resident_identity) return 0;
  const std::lock_guard<std::mutex> lock(mu_);
  const uint32_t* index = index_.Find(key);
  return index != nullptr ? resident_generations_[*index] : 0;
}

uint32_t SlabAllocator::AcquireSlotMetaLocked() {
  if (free_slot_meta_head_ != kNoSlot) {
    const uint32_t index = free_slot_meta_head_;
    free_slot_meta_head_ = slots_[index].free_next;
    slots_[index] = SlotMeta{};
    if (opt_.track_resident_identity) resident_generations_[index] = 0;
    if (opt_.track_lifetime_state) {
      resident_lifetime_states_[index].reset();
      if (opt_.lifetime_bucket_count != 0) lifetime_bucket_tokens_[index] = 0;
    }
    return index;
  }
  slots_.emplace_back();
  if (opt_.track_resident_identity) resident_generations_.push_back(0);
  if (opt_.track_lifetime_state) {
    resident_lifetime_states_.emplace_back();
    lifetime_links_.emplace_back();
    if (opt_.lifetime_bucket_count != 0) lifetime_bucket_tokens_.push_back(0);
  }
  return static_cast<uint32_t>(slots_.size() - 1);
}

void SlabAllocator::RegisterLifetimeBucketLocked(
    uint32_t index, const DiskLifetimeConfig& config, uint32_t now) {
  if (opt_.lifetime_bucket_count == 0) return;
  assert(lifetime_bucket_tokens_[index] == 0);
  const std::optional<DiskLifetimeState>& resident_state =
      resident_lifetime_states_[index];
  assert(config.bucket_sec == opt_.lifetime_bucket_sec &&
         config.bucket_count == opt_.lifetime_bucket_count);
  if (!resident_state.has_value()) return;  // Unknown is not a scoring state.
  const DiskLifetimeState& state = *resident_state;
  const uint64_t end =
      DiskLifetimeProtectEnd(config, state.anchor, state.scoring_reads(),
                             state.first_early_read(), state.write_armed());
  if (end <= now) return;  // An expired recovered resident never registered.
  // Use the page containing the last protected second. An exact boundary
  // expires there, while other deadlines are conservatively rounded upward.
  const uint64_t generation = (end - 1) / opt_.lifetime_bucket_sec + 1;
  const size_t page = (generation - 1) % opt_.lifetime_bucket_count;
  LifetimeBucket& bucket =
      lifetime_buckets_[static_cast<size_t>(slots_[index].ref.extent) *
                            opt_.lifetime_bucket_count +
                        page];
  if (bucket.generation != generation) {
    // Coverage B*G >= H excludes collisions between live pages. A lazy reset
    // may discard only an expired page, never another protected generation.
    assert(bucket.count == 0 ||
           bucket.generation <= now / opt_.lifetime_bucket_sec);
    bucket = {generation, 0};
  }
  ++bucket.count;
  LifetimeBucket& summary = lifetime_bucket_summary_[page];
  if (summary.generation < generation) {
    assert(summary.count == 0 ||
           summary.generation <= now / opt_.lifetime_bucket_sec);
    summary = {generation, 0};
  }
  // A delayed older generation is already expired at the current owner time.
  // Never replace the latest aggregate generation, even when its count is zero.
  if (summary.generation == generation) ++summary.count;
  lifetime_bucket_tokens_[index] = generation;
}

void SlabAllocator::UnregisterLifetimeBucketLocked(uint32_t index) {
  if (opt_.lifetime_bucket_count == 0) return;
  const uint64_t generation = lifetime_bucket_tokens_[index];
  if (generation == 0) return;  // Never infer registration from old heat state.
  lifetime_bucket_tokens_[index] = 0;
  const size_t page = (generation - 1) % opt_.lifetime_bucket_count;
  LifetimeBucket& bucket =
      lifetime_buckets_[static_cast<size_t>(slots_[index].ref.extent) *
                            opt_.lifetime_bucket_count +
                        page];
  if (bucket.generation == generation) {
    assert(bucket.count != 0);
    --bucket.count;
    LifetimeBucket& summary = lifetime_bucket_summary_[page];
    if (summary.generation == generation) {
      assert(summary.count != 0);
      --summary.count;
    }
  }  // An old token must not subtract a newer occupant of the ring position.
}

void SlabAllocator::ResetLifetimeBucketsLocked(uint32_t extent) {
  if (opt_.lifetime_bucket_count == 0) return;
  const size_t first = static_cast<size_t>(extent) * opt_.lifetime_bucket_count;
  for (uint32_t page = 0; page < opt_.lifetime_bucket_count; ++page) {
    LifetimeBucket& bucket = lifetime_buckets_[first + page];
    LifetimeBucket& summary = lifetime_bucket_summary_[page];
    if (bucket.generation == summary.generation) {
      assert(summary.count >= bucket.count);
      summary.count -= bucket.count;
    }
    bucket = LifetimeBucket{};
  }
}

uint64_t SlabAllocator::LifetimeProtectedEstimateLocked(uint32_t extent,
                                                        uint64_t now) const {
  if (opt_.lifetime_bucket_count == 0 || extent >= extents_.size()) return 0;
  const uint64_t current_page = now / opt_.lifetime_bucket_sec;
  const size_t first = static_cast<size_t>(extent) * opt_.lifetime_bucket_count;
  uint64_t count = 0;
  for (uint32_t page = 0; page < opt_.lifetime_bucket_count; ++page) {
    const LifetimeBucket& bucket = lifetime_buckets_[first + page];
    if (bucket.generation > current_page) count += bucket.count;
  }
  return count;  // Approximate only; query never rotates or clears pages.
}

SlabAllocator::LifetimeQueue& SlabAllocator::LifetimeQueueFor(
    uint32_t meta_index, size_t index, uint32_t band) {
  const size_t row = index == 0 ? 0 : slots_[meta_index].ref.cls + 1;
  return lifetime_queues_[row * (opt_.lifetime_max_reads + 1) + band];
}

void SlabAllocator::LinkLifetimeLocked(uint32_t meta_index) {
  LifetimeLinks& links = lifetime_links_[meta_index];
  assert(links.band == kNoSlot);
  const DiskLifetimeState& state = *resident_lifetime_states_[meta_index];
  // fw contributes one effective scoring read; widen before adding, matching
  // DiskLifetimeBand rather than the raw six-bit read counter alone.
  BumpLifetimeVersionLocked(meta_index);
  links.band = std::min<uint32_t>(
      static_cast<uint32_t>(state.scoring_reads()) + state.first_early_read(),
      opt_.lifetime_max_reads);
  for (size_t index = 0; index < links.indexes.size(); ++index) {
    LifetimeQueue& queue = LifetimeQueueFor(meta_index, index, links.band);
    // Actual commits/reads normally append in O(1). Recovery or delayed commit
    // publication may have an older anchor; keep the same sorted invariant in
    // both indexes instead of assuming enumeration order is chronological.
    uint32_t prev = queue.tail;
    while (prev != kNoSlot &&
           resident_lifetime_states_[prev]->anchor > state.anchor) {
      prev = lifetime_links_[prev].indexes[index].prev;
    }
    const uint32_t next = prev == kNoSlot
                              ? queue.head
                              : lifetime_links_[prev].indexes[index].next;
    links.indexes[index] = {prev, next};
    if (prev == kNoSlot)
      queue.head = meta_index;
    else
      lifetime_links_[prev].indexes[index].next = meta_index;
    if (next == kNoSlot)
      queue.tail = meta_index;
    else
      lifetime_links_[next].indexes[index].prev = meta_index;
    if (queue.cursor == kNoSlot) queue.cursor = meta_index;
    ++queue.residents;
  }
}

void SlabAllocator::UnlinkLifetimeLocked(uint32_t meta_index) {
  LifetimeLinks& links = lifetime_links_[meta_index];
  if (links.band == kNoSlot) return;
  BumpLifetimeVersionLocked(meta_index);
  for (size_t index = 0; index < links.indexes.size(); ++index) {
    LifetimeQueue& queue = LifetimeQueueFor(meta_index, index, links.band);
    const LifetimeLink link = links.indexes[index];
    if (link.prev == kNoSlot)
      queue.head = link.next;
    else
      lifetime_links_[link.prev].indexes[index].next = link.next;
    if (link.next == kNoSlot)
      queue.tail = link.prev;
    else
      lifetime_links_[link.next].indexes[index].prev = link.prev;
    // A continuation points to a surviving successor, wrapping only at tail.
    if (queue.cursor == meta_index)
      queue.cursor = link.next == kNoSlot ? queue.head : link.next;
    assert(queue.residents != 0);
    --queue.residents;
  }
  links = LifetimeLinks{};
}

void SlabAllocator::LinkBeforeHandLocked(uint32_t index) {
  SlotMeta& meta = slots_[index];
  Class& cls = classes_[meta.ref.cls];

  // Behind the hand in forward sweep order: visit older residents first.
  if (cls.hand == kNoSlot) {
    cls.ring_head = cls.hand = index;
  } else {
    meta.class_next = cls.hand;
    meta.class_prev = slots_[cls.hand].class_prev;
    if (meta.class_prev != kNoSlot)
      slots_[meta.class_prev].class_next = index;
    else
      cls.ring_head = index;
    slots_[cls.hand].class_prev = index;
  }
}

void SlabAllocator::LinkBeforeGlobalHandLocked(uint32_t index) {
  SlotMeta& meta = slots_[index];
  if (global_hand_ == kNoSlot) {
    global_ring_head_ = global_hand_ = index;
  } else {
    meta.global_next = global_hand_;
    meta.global_prev = slots_[global_hand_].global_prev;
    if (meta.global_prev != kNoSlot)
      slots_[meta.global_prev].global_next = index;
    else
      global_ring_head_ = index;
    slots_[global_hand_].global_prev = index;
  }
}

void SlabAllocator::LinkResidentLocked(uint32_t index) {
  SlotMeta& meta = slots_[index];
  Class& cls = classes_[meta.ref.cls];
  ExtentMeta& extent = extents_[meta.ref.extent];

  LinkBeforeHandLocked(index);
  LinkBeforeGlobalHandLocked(index);
  ++cls.resident_count;

  meta.extent_next = extent.resident_head;
  if (extent.resident_head != kNoSlot)
    slots_[extent.resident_head].extent_prev = index;
  extent.resident_head = index;
  ++extent.resident_count;
}

uint64_t SlabAllocator::DecayedHeat(uint64_t heat, uint64_t heat_seq) const {
  if (heat == 0) return 0;
  const uint64_t elapsed = activity_seq_ >= heat_seq ? activity_seq_ - heat_seq : 0;
  const uint64_t shifts = elapsed / kReadDecayOps;
  return shifts >= 63 ? 0 : heat >> shifts;
}

uint64_t SlabAllocator::ExtentHeatLocked(const ExtentMeta& extent) const {
  return DecayedHeat(extent.read_heat, extent.heat_seq);
}

void SlabAllocator::TouchLocked(uint32_t index) {
  SlotMeta& meta = slots_[index];
  ++activity_seq_;
  meta.referenced = true;
  ++meta.read_touches;

  Class& cls = classes_[meta.ref.cls];
  // A reference bit alone forgets actual read order after one sweep. Refresh
  // the same position used for new writes, without touching extent membership.
  // The predecessor of the hand is already last in sweep order (singleton
  // included), so avoid unlinking it.
  const bool behind_hand =
      meta.class_next == cls.hand ||
      (meta.class_next == kNoSlot && cls.hand == cls.ring_head);
  if (!behind_hand) {
    const uint32_t next = meta.class_next;
    if (meta.class_prev != kNoSlot)
      slots_[meta.class_prev].class_next = next;
    else
      cls.ring_head = next;
    if (next != kNoSlot)
      slots_[next].class_prev = meta.class_prev;
    if (cls.hand == index)
      cls.hand = next != kNoSlot ? next : cls.ring_head;
    LinkBeforeHandLocked(index);
  }

  // Global reclaim uses the same real-read/new-admission order across sizes.
  const bool behind_global_hand =
      meta.global_next == global_hand_ ||
      (meta.global_next == kNoSlot && global_hand_ == global_ring_head_);
  if (!behind_global_hand) {
    const uint32_t next = meta.global_next;
    if (meta.global_prev != kNoSlot)
      slots_[meta.global_prev].global_next = next;
    else
      global_ring_head_ = next;
    if (next != kNoSlot)
      slots_[next].global_prev = meta.global_prev;
    if (global_hand_ == index)
      global_hand_ = next != kNoSlot ? next : global_ring_head_;
    LinkBeforeGlobalHandLocked(index);
  }

  cls.read_heat = DecayedHeat(cls.read_heat, cls.heat_seq) + kReadHeatUnit;
  cls.heat_seq = activity_seq_;
  ++cls.read_touches;

  ExtentMeta& extent = extents_[meta.ref.extent];
  extent.read_heat = ExtentHeatLocked(extent) + kReadHeatUnit;
  extent.heat_seq = activity_seq_;
}

bool SlabAllocator::ReclaimAllowed(bool* reclaim_denied) const {
  if (reclaim_denied != nullptr && *reclaim_denied) return false;
  if (!opt_.can_reclaim || opt_.can_reclaim()) return true;
  if (reclaim_denied != nullptr) *reclaim_denied = true;
  return false;
}

SlabAllocator::ExtentBindResult SlabAllocator::BindExtentLocked(
    size_t cls_index, uint32_t extent_index, bool metadata_prepared,
    bool* reclaim_denied) {
  if (cls_index >= classes_.size() || extent_index >= extents_.size() ||
      extents_[extent_index].cls != kUnbound)
    return ExtentBindResult::kUnavailable;
  Class& cls = classes_[cls_index];
  if (cls.slots_per_extent == 0) return ExtentBindResult::kUnavailable;
  // A successful wipe authorizes its cleanup/final binding even if the guard
  // closes during persistence. Restore also installs prevalidated metadata.
  if (!metadata_prepared && !ReclaimAllowed(reclaim_denied))
    return ExtentBindResult::kReclaimDenied;
  if (!metadata_prepared && opt_.on_extent_bind &&
      !opt_.on_extent_bind(extent_index))
    return ExtentBindResult::kPersistenceError;

  ExtentMeta& extent = extents_[extent_index];
  extent = ExtentMeta{};
  ResetLifetimeBucketsLocked(extent_index);
  extent.cls = static_cast<int>(cls_index);
  extent.total_slots = cls.slots_per_extent;
  extent.free_slots = cls.slots_per_extent;
  ++cls.bound_extents;
  --unbound_;
  for (uint32_t slot = 0; slot < cls.slots_per_extent; ++slot)
    PushFreeLocked(cls, extent_index, slot);
  return ExtentBindResult::kBound;
}

SlabAllocator::ExtentBindResult SlabAllocator::BindFreeExtent(
    size_t cls, bool* reclaim_denied) {
  if (unbound_ == 0) return ExtentBindResult::kUnavailable;
  if (!ReclaimAllowed(reclaim_denied)) return ExtentBindResult::kReclaimDenied;
  for (uint32_t extent = 0; extent < extents_.size(); ++extent) {
    if (extents_[extent].cls == kUnbound)
      return BindExtentLocked(cls, extent, false, reclaim_denied);
  }
  return ExtentBindResult::kUnavailable;
}

bool SlabAllocator::RestoreBulk(const std::vector<RestoreEntry>& entries) {
  std::lock_guard<std::mutex> lock(mu_);
  if (entries.empty()) return true;

  std::vector<BlockKey> keys;
  std::unordered_set<uint64_t> placements;
  std::unordered_map<uint32_t, uint32_t> batch_extent_sizes;
  keys.reserve(entries.size());
  placements.reserve(entries.size());
  batch_extent_sizes.reserve(entries.size());

  for (const RestoreEntry& entry : entries) {
    if (entry.slot_size == 0 || entry.extent >= extents_.size() ||
        entry.slot_size > opt_.extent_bytes ||
        entry.value_bytes > entry.slot_size ||
        CanonicalSlotSize(entry.slot_size) != entry.slot_size ||
        ClassForExactSize(entry.slot_size) == classes_.size() ||
        entry.slot >= opt_.extent_bytes / entry.slot_size ||
        index_.Find(entry.key) != nullptr)
      return false;
    keys.push_back(entry.key);
    const uint64_t placement =
        (static_cast<uint64_t>(entry.extent) << 32) | entry.slot;
    if (!placements.emplace(placement).second) return false;

    auto batch = batch_extent_sizes.emplace(entry.extent, entry.slot_size);
    if (!batch.second && batch.first->second != entry.slot_size) return false;
    const ExtentMeta& extent = extents_[entry.extent];
    if (extent.cls != kUnbound) {
      const Class& cls = classes_[static_cast<size_t>(extent.cls)];
      if (cls.slot_size != entry.slot_size) return false;
      auto free_it = cls.free_by_ext.find(entry.extent);
      if (free_it == cls.free_by_ext.end() ||
          std::find(free_it->second.begin(), free_it->second.end(), entry.slot) ==
              free_it->second.end())
        return false;
    }
  }
  std::sort(keys.begin(), keys.end(), [](const BlockKey& a, const BlockKey& b) {
    if (a.tenant_hash != b.tenant_hash) return a.tenant_hash < b.tenant_hash;
    if (a.digest_hi != b.digest_hi) return a.digest_hi < b.digest_hi;
    return a.digest_lo < b.digest_lo;
  });
  if (std::adjacent_find(keys.begin(), keys.end()) != keys.end()) return false;

  slots_.reserve(slots_.size() + entries.size());
  if (opt_.track_resident_identity)
    resident_generations_.reserve(slots_.size() + entries.size());
  if (opt_.track_lifetime_state) {
    resident_lifetime_states_.reserve(slots_.size() + entries.size());
    lifetime_links_.reserve(slots_.size() + entries.size());
    if (opt_.lifetime_bucket_count != 0)
      lifetime_bucket_tokens_.reserve(slots_.size() + entries.size());
  }
  index_.Reserve(index_.size() + entries.size());


  for (const RestoreEntry& entry : entries) {
    const size_t cls_index = ClassForExactSize(entry.slot_size);
    if (extents_[entry.extent].cls == kUnbound) {
      if (BindExtentLocked(cls_index, entry.extent, true) !=
          ExtentBindResult::kBound)
        return false;
    }
    Class& cls = classes_[cls_index];
    if (!TakeFreeSlotLocked(cls, entry.extent, entry.slot)) return false;

    const uint32_t meta_index = AcquireSlotMetaLocked();
    SlotMeta& meta = slots_[meta_index];
    meta.ref.cls = static_cast<uint32_t>(cls_index);
    meta.ref.extent = entry.extent;
    meta.ref.slot = entry.slot;
    meta.ref.slot_size = entry.slot_size;
    meta.ref.offset = static_cast<uint64_t>(entry.slot) * entry.slot_size;
    meta.useful_bytes = entry.value_bytes;
    meta.key = entry.key;
    meta.live = true;
    if (!index_.Insert(entry.key, meta_index)) return false;
    LinkResidentLocked(meta_index);

    ExtentMeta& extent = extents_[entry.extent];
    --extent.free_slots;
    extent.useful_bytes += meta.useful_bytes;
    cls.useful_bytes += meta.useful_bytes;
    used_bytes_ += entry.slot_size;
  }
  return true;
}

void SlabAllocator::FreeSlotLocked(uint32_t index) {
  if (opt_.track_lifetime_state) {
    UnregisterLifetimeBucketLocked(index);
    UnlinkLifetimeLocked(index);
  }
  SlotMeta& meta = slots_[index];
  const uint32_t cls_index = meta.ref.cls;
  const uint32_t extent_index = meta.ref.extent;
  const uint32_t physical_slot = meta.ref.slot;
  const uint32_t slot_size = meta.ref.slot_size;
  const uint64_t useful_bytes = meta.useful_bytes;
  Class& cls = classes_[cls_index];
  ExtentMeta& extent = extents_[extent_index];

  const uint32_t class_next = meta.class_next;
  if (meta.class_prev != kNoSlot)
    slots_[meta.class_prev].class_next = meta.class_next;
  else
    cls.ring_head = meta.class_next;
  if (meta.class_next != kNoSlot)
    slots_[meta.class_next].class_prev = meta.class_prev;
  --cls.resident_count;
  if (cls.hand == index) {
    cls.hand = class_next != kNoSlot ? class_next : cls.ring_head;
  }
  if (cls.resident_count == 0) cls.hand = kNoSlot;

  const uint32_t global_next = meta.global_next;
  if (meta.global_prev != kNoSlot)
    slots_[meta.global_prev].global_next = global_next;
  else
    global_ring_head_ = global_next;
  if (global_next != kNoSlot)
    slots_[global_next].global_prev = meta.global_prev;
  if (global_hand_ == index)
    global_hand_ = global_next != kNoSlot ? global_next : global_ring_head_;

  if (meta.extent_prev != kNoSlot)
    slots_[meta.extent_prev].extent_next = meta.extent_next;
  else
    extent.resident_head = meta.extent_next;
  if (meta.extent_next != kNoSlot)
    slots_[meta.extent_next].extent_prev = meta.extent_prev;
  --extent.resident_count;

  if (meta.refs > 0 && extent.pinned > 0) --extent.pinned;
  index_.Erase(meta.key);
  if (opt_.track_resident_identity) resident_generations_[index] = 0;
  if (opt_.track_lifetime_state) {
    const std::optional<DiskLifetimeState>& state =
        resident_lifetime_states_[index];
    if (state.has_value() && state->history_unknown) {
      // Visibility loss and pending Remove do not release this resident.
      // All reclaim paths debit here only after persistence permits reuse.
      assert(cls.recovered_residents != 0);
      --cls.recovered_residents;
      if (state->actual_reads == 0) {
        assert(cls.recovered_unread_residents != 0);
        --cls.recovered_unread_residents;
      }
    }
    resident_lifetime_states_[index].reset();
  }
  PushFreeLocked(cls, extent_index, physical_slot);
  ++extent.free_slots;
  extent.useful_bytes -= useful_bytes;
  cls.useful_bytes -= useful_bytes;
  used_bytes_ -= slot_size;
  meta = SlotMeta{};
  meta.free_next = free_slot_meta_head_;
  free_slot_meta_head_ = index;

  if (extent.free_slots == extent.total_slots &&
      cls.ext_rr.size() > kStripeWays) {
    DropExtentFreeLocked(cls, extent_index);
    --cls.bound_extents;
    extent = ExtentMeta{};
    ResetLifetimeBucketsLocked(extent_index);
    ++unbound_;
    ++extent_returns_;
  }
}

bool SlabAllocator::EvictOneFrom(size_t cls_index,
                                 std::vector<BlockKey>* evicted,
                                 bool* reclaim_denied) {
  Class& cls = classes_[cls_index];
  if (cls.resident_count == 0) return false;
  const size_t limit = 2 * cls.resident_count + 2;
  uint32_t fallback = kNoSlot;
  for (size_t steps = 0; steps < limit && cls.resident_count != 0; ++steps) {
    if (!ReclaimAllowed(reclaim_denied)) return false;
    if (cls.hand == kNoSlot) cls.hand = cls.ring_head;
    const uint32_t current = cls.hand;
    SlotMeta& meta = slots_[current];
    cls.hand = meta.class_next != kNoSlot ? meta.class_next : cls.ring_head;
    if (meta.refs > 0) continue;
    if (fallback == kNoSlot) fallback = current;
    if (meta.referenced) {
      meta.referenced = false;
      continue;
    }
    if (!ReclaimAllowed(reclaim_denied)) return false;
    if (opt_.on_slot_evict && !opt_.on_slot_evict(meta.ref)) return false;
    BlockKey victim = meta.key;
    FreeSlotLocked(current);
    if (evicted) evicted->push_back(std::move(victim));
    ++evictions_;
    return true;
  }
  if (fallback != kNoSlot && slots_[fallback].live) {
    SlotMeta& meta = slots_[fallback];
    if (!ReclaimAllowed(reclaim_denied)) return false;
    if (opt_.on_slot_evict && !opt_.on_slot_evict(meta.ref)) return false;
    BlockKey victim = meta.key;
    FreeSlotLocked(fallback);
    if (evicted) evicted->push_back(std::move(victim));
    ++evictions_;
    return true;
  }
  return false;
}

void SlabAllocator::BumpLifetimeVersionLocked(uint32_t meta_index) {
  if (!opt_.track_lifetime_state) return;
  uint64_t& version = lifetime_versions_[slots_[meta_index].ref.cls];
  // Saturation forces every later slice to rebuild progress, never an ABA wrap.
  if (version != UINT64_MAX) ++version;
}

bool SlabAllocator::EvictLifetimeFromLocked(size_t cls_index,
                                            LifetimeScan& scan,
                                            std::vector<BlockKey>* evicted,
                                            bool* reclaim_denied,
                                            uint32_t max_visits) {
  const Class& cls = classes_[cls_index];
  if (!scan.initialized) {
    const uint64_t residents = cls.resident_count;
    if (residents > (UINT64_MAX - 2) / 2) {
      scan.result = LifetimeScanResult::kBudgetExhausted;
      return false;
    }
    scan.visit_limit = 2 * residents + 2;
    scan.slice_limit =
        scan.visit_limit / 1024 + (scan.visit_limit % 1024 != 0) + 1;
    scan.initialized = true;
  }
  if (scan.visits >= scan.visit_limit || scan.slices >= scan.slice_limit) {
    scan.result = LifetimeScanResult::kBudgetExhausted;
    return false;
  }
  ++scan.slices;  // An empty or version-retry slice still costs an opportunity.
  if (!ReclaimAllowed(reclaim_denied)) return false;
  uint32_t now = 0;
  if (!scan.clock.TryDrainNow(&now)) {
    *reclaim_denied = true;
    return false;
  }
  const uint64_t version = lifetime_versions_[cls_index];
  const size_t bands = opt_.lifetime_max_reads + 1;
  if (!scan.progress_initialized || version == UINT64_MAX ||
      scan.version != version) {
    // Saved positions are usable only while membership, heat and pin
    // eligibility are unchanged. Other owners may move the shared cursor;
    // this owner's own cursors prevent that from faking complete coverage.
    for (size_t band = 0; band < bands; ++band) {
      const LifetimeQueue& queue =
          lifetime_queues_[(cls_index + 1) * bands + band];
      scan.starts[band] = queue.cursor;
      scan.cursors[band] = queue.cursor;
      scan.done[band] = queue.cursor == kNoSlot;
    }
    scan.version = version;
    scan.progress_initialized = true;
  }
  uint32_t best = kNoSlot;
  int64_t best_score = 0;
  const uint32_t slice_limit = std::min<uint32_t>(1024, max_visits);
  uint32_t slice_visits = 0;
  bool pending = true;
  while (pending && slice_visits < slice_limit &&
         scan.visits < scan.visit_limit) {
    pending = false;
    for (size_t band = 0; band < bands && slice_visits < slice_limit &&
                          scan.visits < scan.visit_limit;
         ++band) {
      if (scan.done[band]) continue;
      const uint32_t current = scan.cursors[band];
      const SlotMeta& meta = slots_[current];
      LifetimeQueue& queue = lifetime_queues_[(cls_index + 1) * bands + band];
      const uint32_t next = lifetime_links_[current].indexes[1].next;
      scan.cursors[band] = next == kNoSlot ? queue.head : next;
      queue.cursor = scan.cursors[band];
      ++slice_visits;
      ++scan.visits;
      scan.done[band] = scan.cursors[band] == scan.starts[band];
      if (meta.live && !meta.remove_pending && meta.refs == 0 &&
          resident_generations_[current] != 0 &&
          resident_lifetime_states_[current].has_value()) {
        const DiskLifetimeState& state = *resident_lifetime_states_[current];
        const int64_t score = DiskLifetimeScore(
            scan.config, state.anchor, state.scoring_reads(),
            state.first_early_read(), state.write_armed(), now);
        // Generations are unique for this store instance and never reused.
        if (best == kNoSlot || score < best_score ||
            (score == best_score &&
             (state.anchor < resident_lifetime_states_[best]->anchor ||
              (state.anchor == resident_lifetime_states_[best]->anchor &&
               resident_generations_[current] <
                   resident_generations_[best])))) {
          best = current;
          best_score = score;
        }
        scan.done[band] = true;  // First checked eligible member in this band.
      }
      pending = pending || !scan.done[band];
    }
  }
  if (best != kNoSlot) {
    if (!ReclaimAllowed(reclaim_denied)) return false;
    // No lock boundary since scoring: pin, generation and membership cannot
    // have changed. Persist exactly one invalidation before freeing anything.
    if (opt_.on_slot_evict && !opt_.on_slot_evict(slots_[best].ref)) {
      scan.result = LifetimeScanResult::kPersistenceError;
      return false;
    }
    const BlockKey victim = slots_[best].key;
    const uint32_t slot_bytes = slots_[best].ref.slot_size;
    const uint64_t payload_bytes = slots_[best].useful_bytes;
    FreeSlotLocked(best);
    if (evicted != nullptr) evicted->push_back(victim);
    ++evictions_;
    ++scan.victims;
    scan.freed_slot_bytes += slot_bytes;
    scan.freed_payload_bytes += payload_bytes;
    scan.result = LifetimeScanResult::kEvicted;
    return true;
  }
  // A loop may stop at its visit cap before reaching a later pending band.
  pending = false;
  for (size_t band = 0; band < bands; ++band) pending |= !scan.done[band];
  if (!pending)
    scan.result = LifetimeScanResult::kNoCandidate;
  else if (scan.visits >= scan.visit_limit || scan.slices >= scan.slice_limit)
    scan.result = LifetimeScanResult::kBudgetExhausted;
  else
    scan.result = LifetimeScanResult::kRetry;
  return false;
}

SlabAllocator::ExtentBindResult SlabAllocator::StealLifetimeDonorLocked(
    size_t target_cls, size_t donor_cls, size_t min_donor_extents,
    uint64_t cold_before, std::vector<BlockKey>* evicted,
    bool* reclaim_denied) {
  if (!ReclaimAllowed(reclaim_denied)) return ExtentBindResult::kReclaimDenied;
  const DiskLifetimeConfig& config = *opt_.lifetime_donor_config;
  const DiskLifetimeClock& clock = *opt_.lifetime_donor_clock;
  uint32_t now = 0;
  if (!clock.TryDrainNow(&now)) {
    if (reclaim_denied != nullptr) *reclaim_denied = true;
    return ExtentBindResult::kReclaimDenied;
  }
  const std::chrono::steady_clock::time_point started =
      std::chrono::steady_clock::now();
  struct Candidate {
    uint32_t extent = kNoSlot;
    uint64_t protected_count = 0;
    uint64_t protected_bytes = 0;
  };
  const auto coarse_less = [this](const Candidate& left,
                                  const Candidate& right) {
    const ExtentMeta& a = extents_[left.extent];
    const ExtentMeta& b = extents_[right.extent];
    return std::make_tuple(left.protected_count, a.useful_bytes,
                           UINT32_MAX - a.free_slots, left.extent) <
           std::make_tuple(right.protected_count, b.useful_bytes,
                           UINT32_MAX - b.free_slots, right.extent);
  };
  const auto precise_less = [this](const Candidate& left,
                                   const Candidate& right) {
    const ExtentMeta& a = extents_[left.extent];
    const ExtentMeta& b = extents_[right.extent];
    return std::make_tuple(left.protected_count, left.protected_bytes,
                           a.useful_bytes, UINT32_MAX - a.free_slots,
                           left.extent) <
           std::make_tuple(right.protected_count, right.protected_bytes,
                           b.useful_bytes, UINT32_MAX - b.free_slots,
                           right.extent);
  };
  constexpr size_t kMaxExtents = 8;
  constexpr uint32_t kMaxResidents = 8192;
  std::array<Candidate, kMaxExtents> candidates{};
  size_t count = 0;
  Candidate fallback;
  // Visit the entire configured pool exactly once. Keeping only the eight
  // best individually inspectable candidates costs O(8*E), not an E-sized
  // allocation/sort, and cannot hide a legal donor beyond a fixed prefix.
  for (uint32_t e = 0; e < extents_.size(); ++e) {
    ++lifetime_donor_stats_.coarse_extents;
    const ExtentMeta& extent = extents_[e];
    if (extent.cls == kUnbound || extent.cls == static_cast<int>(target_cls) ||
        extent.pinned != 0 ||
        (donor_cls < classes_.size() &&
         extent.cls != static_cast<int>(donor_cls)) ||
        classes_[static_cast<size_t>(extent.cls)].bound_extents <=
            min_donor_extents ||
        (cold_before != 0 && extent.youngest_seq >= cold_before))
      continue;
    const Candidate candidate{e, LifetimeProtectedEstimateLocked(e, now), 0};
    if (fallback.extent == kNoSlot || coarse_less(candidate, fallback))
      fallback = candidate;
    if (extent.resident_count > kMaxResidents) continue;
    size_t position = 0;
    while (position < count && !coarse_less(candidate, candidates[position]))
      ++position;
    if (position == kMaxExtents) continue;
    count = std::min(count + 1, kMaxExtents);
    for (size_t at = count - 1; at > position; --at)
      candidates[at] = candidates[at - 1];
    candidates[position] = candidate;
  }
  Candidate best;
  uint32_t visits = 0;
  for (size_t at = 0; at < count; ++at) {
    Candidate candidate = candidates[at];
    const ExtentMeta& extent = extents_[candidate.extent];
    if (extent.resident_count > kMaxResidents - visits) break;
    candidate.protected_count = 0;
    uint32_t current = extent.resident_head;
    uint32_t inspected = 0;
    bool complete = true;
    while (current != kNoSlot && inspected < extent.resident_count) {
      ++inspected;
      ++visits;
      ++lifetime_donor_stats_.precise_residents;
      const SlotMeta& meta = slots_[current];
      const std::optional<DiskLifetimeState>& state =
          resident_lifetime_states_[current];
      if (!meta.live || resident_generations_[current] == 0 ||
          !state.has_value()) {
        complete = false;  // Unknown is not an exactly scored resident.
        break;
      }
      if (DiskLifetimeScore(config, state->anchor, state->scoring_reads(),
                            state->first_early_read(), state->write_armed(),
                            now) >
          static_cast<int64_t>(config.keep_score_sec)) {
        ++candidate.protected_count;
        candidate.protected_bytes += meta.useful_bytes;
      }
      current = meta.extent_next;
    }
    if (!complete || current != kNoSlot || inspected != extent.resident_count)
      continue;  // No partial extent may enter the exact ranking.
    ++lifetime_donor_stats_.precise_extents;
    if (best.extent == kNoSlot || precise_less(candidate, best))
      best = candidate;
  }
  if (best.extent == kNoSlot && fallback.extent != kNoSlot) {
    best = fallback;
    ++lifetime_donor_stats_.budget_fallback;
  }
  lifetime_donor_stats_.selection_ns += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - started)
          .count());
  if (best.extent == kNoSlot) {
    ++lifetime_donor_stats_.unavailable;
    return ExtentBindResult::kUnavailable;
  }
  // No candidate crosses an A-lock boundary. Recheck health/time immediately
  // before beginning the only persistence attempt selected by this decision.
  if (!ReclaimAllowed(reclaim_denied) || !clock.TryDrainNow(&now)) {
    if (reclaim_denied != nullptr) *reclaim_denied = true;
    return ExtentBindResult::kReclaimDenied;
  }
  const ExtentMeta& donor = extents_[best.extent];
  const uint32_t residents = donor.resident_count;
  // Snapshot occupied bytes before release clears the donor. Free capacity and
  // the incoming allocation do not belong to this successful eviction event.
  const uint64_t slot_bytes =
      static_cast<uint64_t>(residents) *
      classes_[static_cast<size_t>(donor.cls)].slot_size;
  const uint64_t payload_bytes = donor.useful_bytes;
  const std::chrono::steady_clock::time_point persistence_started =
      std::chrono::steady_clock::now();
  const ExtentBindResult result =
      StealExtentLocked(best.extent, target_cls, evicted, now, reclaim_denied);
  // Whole-extent invalidation/sync and release are separate from the bounded
  // selection work. Their cost may exceed the 8192-resident inspection cap.
  lifetime_donor_stats_.persistence_ns += static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - persistence_started)
          .count());
  if (result == ExtentBindResult::kPersistenceError)
    ++lifetime_donor_stats_.persistence_errors;
  if (result == ExtentBindResult::kBound) {
    lifetime_donor_stats_.released_residents += residents;
    lifetime_donor_stats_.freed_slot_bytes += slot_bytes;
    lifetime_donor_stats_.freed_payload_bytes += payload_bytes;
  }
  return result;
}

SlabAllocator::ExtentBindResult SlabAllocator::StealExtentFor(
    size_t target_cls, std::vector<BlockKey>* evicted, size_t min_donor_extents,
    bool* reclaim_denied) {
  if (opt_.lifetime_donor_config != nullptr)
    return StealLifetimeDonorLocked(target_cls, classes_.size(),
                                    min_donor_extents, 0, evicted,
                                    reclaim_denied);
  if (!ReclaimAllowed(reclaim_denied)) return ExtentBindResult::kReclaimDenied;
  int best = -1;
  uint64_t best_heat = std::numeric_limits<uint64_t>::max();
  uint64_t best_useful = std::numeric_limits<uint64_t>::max();
  uint32_t best_free = 0;
  for (uint32_t e = 0; e < extents_.size(); ++e) {
    const ExtentMeta& extent = extents_[e];
    if (extent.cls == kUnbound || extent.cls == static_cast<int>(target_cls) ||
        extent.pinned != 0)
      continue;
    const Class& donor = classes_[static_cast<size_t>(extent.cls)];
    if (min_donor_extents > 0 && donor.bound_extents <= min_donor_extents)
      continue;
    const uint64_t heat = ExtentHeatLocked(extent);
    if (best < 0 || heat < best_heat ||
        (heat == best_heat && extent.useful_bytes < best_useful) ||
        (heat == best_heat && extent.useful_bytes == best_useful &&
         extent.free_slots > best_free)) {
      best = static_cast<int>(e);
      best_heat = heat;
      best_useful = extent.useful_bytes;
      best_free = extent.free_slots;
    }
  }
  if (best < 0) return ExtentBindResult::kUnavailable;
  return StealExtentLocked(static_cast<uint32_t>(best), target_cls, evicted, 0,
                           reclaim_denied);
}

SlabAllocator::ExtentBindResult SlabAllocator::StealColdExtentFor(
    size_t target_cls, std::vector<BlockKey>* evicted, bool* reclaim_denied) {
  if (!cold_steal_enabled_) return ExtentBindResult::kUnavailable;
  if (!ReclaimAllowed(reclaim_denied)) return ExtentBindResult::kReclaimDenied;
  const uint64_t window = cold_window_ ? cold_window_ : index_.size();
  if (window == 0 || put_seq_ <= window) return ExtentBindResult::kUnavailable;
  const uint64_t cold_before = put_seq_ - window;
  if (opt_.lifetime_donor_config != nullptr) {
    const ExtentBindResult result = StealLifetimeDonorLocked(
        target_cls, classes_.size(), 0, cold_before, evicted, reclaim_denied);
    if (result == ExtentBindResult::kBound) ++cold_steals_;
    return result;
  }
  int best = -1;
  uint64_t best_heat = std::numeric_limits<uint64_t>::max();
  uint64_t best_useful = std::numeric_limits<uint64_t>::max();
  uint64_t best_seq = std::numeric_limits<uint64_t>::max();
  for (uint32_t e = 0; e < extents_.size(); ++e) {
    const ExtentMeta& extent = extents_[e];
    if (extent.cls == kUnbound || extent.cls == static_cast<int>(target_cls) ||
        extent.pinned != 0 || extent.resident_count == 0 ||
        extent.youngest_seq >= cold_before)
      continue;
    const uint64_t heat = ExtentHeatLocked(extent);
    if (best < 0 || heat < best_heat ||
        (heat == best_heat && extent.useful_bytes < best_useful) ||
        (heat == best_heat && extent.useful_bytes == best_useful &&
         extent.youngest_seq < best_seq)) {
      best = static_cast<int>(e);
      best_heat = heat;
      best_useful = extent.useful_bytes;
      best_seq = extent.youngest_seq;
    }
  }
  if (best < 0) return ExtentBindResult::kUnavailable;
  const ExtentBindResult result = StealExtentLocked(
      static_cast<uint32_t>(best), target_cls, evicted, 0, reclaim_denied);
  if (result == ExtentBindResult::kBound) ++cold_steals_;
  return result;
}

SlabAllocator::ExtentBindResult SlabAllocator::StealExtentLocked(
    uint32_t extent_index, size_t target_cls, std::vector<BlockKey>* evicted,
    uint32_t lifetime_now, bool* reclaim_denied) {
  ExtentMeta& extent = extents_[extent_index];
  const int old_cls = extent.cls;
  if (old_cls == kUnbound || target_cls >= classes_.size() ||
      classes_[target_cls].slots_per_extent == 0 || extent.pinned != 0)
    return ExtentBindResult::kUnavailable;

  // The durable extent wipe happens before any resident is forgotten.
  if (!ReclaimAllowed(reclaim_denied)) return ExtentBindResult::kReclaimDenied;
  if (opt_.on_extent_bind && !opt_.on_extent_bind(extent_index))
    return ExtentBindResult::kPersistenceError;
  while (extent.resident_head != kNoSlot) {
    const uint32_t index = extent.resident_head;
    BlockKey victim = slots_[index].key;
    if (opt_.lifetime_donor_config != nullptr) {
      // The wipe succeeded, so this resident will be released. Use the final
      // pre-wipe time even if I/O crossed KEEP; never reuse a bucket estimate.
      const DiskLifetimeConfig& config = *opt_.lifetime_donor_config;
      const std::optional<DiskLifetimeState>& state =
          resident_lifetime_states_[index];
      if (!state.has_value() || resident_generations_[index] == 0) {
        ++lifetime_donor_stats_.unscored_victims;
      } else if (DiskLifetimeScore(config, state->anchor,
                                   state->scoring_reads(),
                                   state->first_early_read(),
                                   state->write_armed(), lifetime_now) >
                 static_cast<int64_t>(config.keep_score_sec)) {
        ++lifetime_donor_stats_.protected_victims;
        lifetime_donor_stats_.protected_payload_bytes +=
            slots_[index].useful_bytes;
      }
    }
    FreeSlotLocked(index);
    if (evicted) evicted->push_back(std::move(victim));
    ++evictions_;
  }
  if (extents_[extent_index].cls != kUnbound) {
    DropExtentFreeLocked(classes_[static_cast<size_t>(old_cls)], extent_index);
    --classes_[static_cast<size_t>(old_cls)].bound_extents;
    extents_[extent_index] = ExtentMeta{};
    ResetLifetimeBucketsLocked(extent_index);
    ++unbound_;
  }
  const ExtentBindResult result =
      BindExtentLocked(target_cls, extent_index, true);
  if (result == ExtentBindResult::kBound) ++steals_;
  return result;
}

bool SlabAllocator::StealFrom(size_t donor_cls, size_t target_cls,
                              std::vector<BlockKey>* evicted) {
  return StealFromResult(donor_cls, target_cls, evicted) ==
         ExtentBindResult::kBound;
}

SlabAllocator::ExtentBindResult SlabAllocator::StealFromResult(
    size_t donor_cls, size_t target_cls, std::vector<BlockKey>* evicted,
    size_t min_donor_extents) {
  const std::lock_guard<std::mutex> lock(mu_);
  if (donor_cls == target_cls || donor_cls >= classes_.size() ||
      target_cls >= classes_.size() || classes_[target_cls].slots_per_extent == 0)
    return ExtentBindResult::kUnavailable;
  if (classes_[donor_cls].bound_extents <= min_donor_extents)
    return ExtentBindResult::kUnavailable;
  if (opt_.lifetime_donor_config != nullptr)
    return StealLifetimeDonorLocked(target_cls, donor_cls, min_donor_extents, 0,
                                    evicted);
  if (!ReclaimAllowed()) return ExtentBindResult::kReclaimDenied;
  int best = -1;
  uint64_t best_heat = std::numeric_limits<uint64_t>::max();
  uint64_t best_useful = std::numeric_limits<uint64_t>::max();
  uint32_t best_free = 0;
  for (uint32_t e = 0; e < extents_.size(); ++e) {
    const ExtentMeta& extent = extents_[e];
    if (extent.cls != static_cast<int>(donor_cls) || extent.pinned != 0) continue;
    const uint64_t heat = ExtentHeatLocked(extent);
    if (best < 0 || heat < best_heat ||
        (heat == best_heat && extent.useful_bytes < best_useful) ||
        (heat == best_heat && extent.useful_bytes == best_useful &&
         extent.free_slots > best_free)) {
      best = static_cast<int>(e);
      best_heat = heat;
      best_useful = extent.useful_bytes;
      best_free = extent.free_slots;
    }
  }
  return best >= 0 ? StealExtentLocked(static_cast<uint32_t>(best), target_cls,
                                       evicted, 0)
                   : ExtentBindResult::kUnavailable;
}

bool SlabAllocator::Put(const BlockKey& key, size_t len, SlotRef* out,
                        std::vector<BlockKey>* evicted,
                        SlotHandle* out_handle) {
  return PutImpl(key, len, out, evicted, out_handle, nullptr);
}

bool SlabAllocator::PutImpl(const BlockKey& key, size_t len, SlotRef* out,
                            std::vector<BlockKey>* evicted,
                            SlotHandle* out_handle, bool* reclaim_denied,
                            LifetimeScan* scan) {
  std::lock_guard<std::mutex> lock(mu_);
  bool local_denied = false;
  if (reclaim_denied == nullptr) reclaim_denied = &local_denied;
  *reclaim_denied = false;
  bool class_pending =
      scan != nullptr && scan->result == LifetimeScanResult::kRetry;
  if (scan != nullptr) scan->result = LifetimeScanResult::kIdle;
  if (out_handle) *out_handle = SlotHandle{};
  ++activity_seq_;
  if (uint32_t* existing = index_.Find(key)) {
    SlotMeta& meta = slots_[*existing];
    if (meta.remove_pending) return false;
    meta.referenced = true;
    if (out) *out = meta.ref;
    if (out_handle) out_handle->index = *existing;
    return true;
  }
  if (len == 0 || len > opt_.extent_bytes || len > UINT32_MAX) return false;
  uint64_t aligned = AlignUp(len, opt_.align);
  if (aligned == 0 || aligned > opt_.extent_bytes) return false;

  const size_t cls_index = ClassForLen(static_cast<size_t>(aligned));
  if (cls_index >= classes_.size()) return false;
  Slot placement{0, 0};
  for (;;) {
    Class& cls = classes_[cls_index];
    while (!class_pending && cls.ext_rr.size() < kStripeWays) {
      const ExtentBindResult bind = BindFreeExtent(cls_index, reclaim_denied);
      if (bind == ExtentBindResult::kBound) continue;
      if (bind == ExtentBindResult::kPersistenceError) return false;
      break;
    }
    if (PopFreeLocked(cls, &placement)) {
      *reclaim_denied =
          false;  // The actual allocation no longer needs reclaim.
      break;
    }
    if (*reclaim_denied) return false;
    if (!class_pending && cls.bound_extents < kStripeWays) {
      const ExtentBindResult grow =
          StealExtentFor(cls_index, evicted, kStripeWays, reclaim_denied);
      if (grow == ExtentBindResult::kBound) continue;
      if (grow == ExtentBindResult::kPersistenceError || *reclaim_denied)
        return false;
    }
    if (!class_pending) {
      const ExtentBindResult cold =
          StealColdExtentFor(cls_index, evicted, reclaim_denied);
      if (cold == ExtentBindResult::kBound) continue;
      if (cold == ExtentBindResult::kPersistenceError || *reclaim_denied)
        return false;
    }
    if (scan != nullptr) {
      if (EvictLifetimeFromLocked(cls_index, *scan, evicted, reclaim_denied)) {
        class_pending = false;
        continue;
      }
      if (*reclaim_denied || scan->result != LifetimeScanResult::kNoCandidate)
        return false;
    } else if (EvictOneFrom(cls_index, evicted, reclaim_denied)) {
      continue;
    }
    if (*reclaim_denied) return false;
    const ExtentBindResult steal =
        StealExtentFor(cls_index, evicted, 0, reclaim_denied);
    if (steal == ExtentBindResult::kBound) continue;
    if (steal == ExtentBindResult::kPersistenceError) return false;
    return false;
  }

  const uint32_t meta_index = AcquireSlotMetaLocked();
  SlotMeta& meta = slots_[meta_index];
  meta.ref.cls = static_cast<uint32_t>(cls_index);
  meta.ref.extent = placement.extent;
  meta.ref.slot = placement.slot;
  meta.ref.slot_size = classes_[cls_index].slot_size;
  meta.ref.offset = static_cast<uint64_t>(placement.slot) * meta.ref.slot_size;
  meta.useful_bytes = len;
  const uint64_t insert_seq = ++put_seq_;
  meta.key = key;
  meta.live = true;
  if (!index_.Insert(key, meta_index)) {
    meta = SlotMeta{};
    meta.free_next = free_slot_meta_head_;
    free_slot_meta_head_ = meta_index;
    PushFreeLocked(classes_[cls_index], placement.extent, placement.slot);
    return false;
  }
  LinkResidentLocked(meta_index);

  Class& cls = classes_[cls_index];
  ExtentMeta& extent = extents_[placement.extent];
  ++cls.puts;
  cls.useful_bytes += len;
  --extent.free_slots;
  extent.useful_bytes += len;
  extent.youngest_seq = std::max(extent.youngest_seq, insert_seq);
  used_bytes_ += meta.ref.slot_size;
  if (out) *out = meta.ref;
  if (out_handle) out_handle->index = meta_index;
  return true;
}

bool SlabAllocator::Get(const BlockKey& key, SlotRef* out) {
  std::lock_guard<std::mutex> lock(mu_);
  const uint32_t* index = index_.Find(key);
  if (index == nullptr || slots_[*index].remove_pending) return false;
  TouchLocked(*index);
  if (out) *out = slots_[*index].ref;
  return true;
}

bool SlabAllocator::GetAndPin(const BlockKey& key, SlotRef* out) {
  std::lock_guard<std::mutex> lock(mu_);
  const uint32_t* found = index_.Find(key);
  if (found == nullptr || slots_[*found].remove_pending) return false;
  const uint32_t index = *found;
  TouchLocked(index);
  SlotMeta& meta = slots_[index];
  if (meta.refs == 0) {
    ++extents_[meta.ref.extent].pinned;
    BumpLifetimeVersionLocked(index);
  }
  ++meta.refs;
  if (out) *out = meta.ref;
  return true;
}

bool SlabAllocator::Contains(const BlockKey& key) const {
  std::lock_guard<std::mutex> lock(mu_);
  const uint32_t* index = index_.Find(key);
  return index != nullptr && !slots_[*index].remove_pending;
}

SlabAllocator::RemoveResult SlabAllocator::Remove(const BlockKey& key) {
  std::lock_guard<std::mutex> lock(mu_);
  const uint32_t* found = index_.Find(key);
  if (found == nullptr) return RemoveResult::kNotFound;
  const uint32_t index = *found;
  SlotMeta& meta = slots_[index];
  if (meta.refs > 0) {
    meta.remove_pending = true;
    meta.referenced = false;
    return RemoveResult::kDeferred;
  }
  FreeSlotLocked(index);
  return RemoveResult::kRemoved;
}

bool SlabAllocator::Pin(const BlockKey& key) {
  std::lock_guard<std::mutex> lock(mu_);
  const uint32_t* index = index_.Find(key);
  if (index == nullptr) return false;
  SlotMeta& meta = slots_[*index];
  if (meta.remove_pending) return false;
  if (meta.refs == 0) {
    ++extents_[meta.ref.extent].pinned;
    BumpLifetimeVersionLocked(*index);
  }
  ++meta.refs;
  return true;
}

bool SlabAllocator::Unpin(const BlockKey& key) {
  std::lock_guard<std::mutex> lock(mu_);
  const uint32_t* found = index_.Find(key);
  if (found == nullptr) return false;
  const uint32_t index = *found;
  SlotMeta& meta = slots_[index];
  if (meta.refs == 0) return false;
  --meta.refs;
  if (meta.refs == 0) {
    BumpLifetimeVersionLocked(index);
    if (extents_[meta.ref.extent].pinned > 0) --extents_[meta.ref.extent].pinned;
    if (meta.remove_pending) FreeSlotLocked(index);
  }
  return true;
}

bool SlabAllocator::Pin(const BlockKey& key, SlotHandle handle) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!handle.valid() || handle.index >= slots_.size()) return false;
  SlotMeta& meta = slots_[handle.index];
  if (!meta.live || !(meta.key == key) || meta.remove_pending) return false;
  if (meta.refs == 0) {
    ++extents_[meta.ref.extent].pinned;
    BumpLifetimeVersionLocked(handle.index);
  }
  ++meta.refs;
  return true;
}

bool SlabAllocator::Unpin(const BlockKey& key, SlotHandle handle) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!handle.valid() || handle.index >= slots_.size()) return false;
  SlotMeta& meta = slots_[handle.index];
  if (!meta.live || !(meta.key == key) || meta.refs == 0) return false;
  --meta.refs;
  if (meta.refs == 0) {
    BumpLifetimeVersionLocked(handle.index);
    if (extents_[meta.ref.extent].pinned > 0) --extents_[meta.ref.extent].pinned;
    if (meta.remove_pending) FreeSlotLocked(handle.index);
  }
  return true;
}

std::vector<SlabAllocator::ClassStat> SlabAllocator::Classes() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<ClassStat> result;
  result.reserve(classes_.size());
  for (const Class& cls : classes_) {
    ClassStat stat;
    stat.slot_size = cls.slot_size;
    stat.free_slots = cls.free_count;
    stat.resident = cls.resident_count;
    stat.puts = cls.puts;
    stat.extents = cls.bound_extents;
    stat.slots_per_extent = cls.slots_per_extent;
    stat.useful_bytes = cls.useful_bytes;
    stat.allocated_bytes = static_cast<uint64_t>(cls.resident_count) * cls.slot_size;
    stat.read_touches = cls.read_touches;
    stat.read_heat = DecayedHeat(cls.read_heat, cls.heat_seq);
    stat.recovered_residents = cls.recovered_residents;
    stat.recovered_unread_residents = cls.recovered_unread_residents;
    result.push_back(stat);
  }
  return result;
}

size_t SlabAllocator::ReclaimClass(size_t cls_index, size_t target_free,
                                   size_t max_victims,
                                   std::vector<BlockKey>* evicted) {
  std::lock_guard<std::mutex> lock(mu_);
  if (cls_index >= classes_.size()) return 0;
  Class& cls = classes_[cls_index];
  size_t count = 0;
  while (count < max_victims && cls.free_count < target_free) {
    if (unbound_ > 0) break;
    const size_t before = cls.free_count;
    if (!EvictOneFrom(cls_index, evicted)) break;
    ++count;
    if (cls.free_count <= before) break;
  }
  return count;
}

SlabAllocator::LifetimeReclaimResult SlabAllocator::ReclaimLifetimeClass(
    size_t cls_index, size_t target_free, size_t max_victims,
    LifetimeScan& scan, std::vector<BlockKey>* evicted) {
  const std::lock_guard<std::mutex> lock(mu_);
  LifetimeReclaimResult result;
  if (cls_index >= classes_.size() || max_victims == 0) return result;
  assert(scan.initialized);  // Caller installed one shared background tick cap.
  const Class& cls = classes_[cls_index];
  const uint64_t start_visits = scan.visits;
  while (result.victims < max_victims && cls.free_count < target_free &&
         scan.visits - start_visits < 1024) {
    if (unbound_ > 0) return result;
    const size_t before = cls.free_count;
    bool denied = false;
    const uint32_t remaining =
        1024 - static_cast<uint32_t>(scan.visits - start_visits);
    if (!EvictLifetimeFromLocked(cls_index, scan, evicted, &denied,
                                 remaining)) {
      result.more = !denied && scan.result == LifetimeScanResult::kRetry;
      return result;
    }
    ++result.victims;
    // Class slot size is fixed even when the last victim returns an extent.
    // Count after successful invalidation/free; a later failure retains this.
    result.freed_slot_bytes += cls.slot_size;
    // Match legacy's extent-return guard: do not cascade into another shrink.
    if (cls.free_count <= before) return result;
  }
  result.more = cls.free_count < target_free && unbound_ == 0;
  return result;
}

size_t SlabAllocator::EvictLifetimeToTarget(uint64_t target_bytes,
                                            size_t max_victims,
                                            const DiskLifetimeConfig& config,
                                            const DiskLifetimeClock& clock,
                                            std::vector<BlockKey>* evicted,
                                            LifetimeWatermarkStats* stats) {
  const std::lock_guard<std::mutex> lock(mu_);
  if (stats != nullptr) *stats = LifetimeWatermarkStats{};
  if (!opt_.track_lifetime_state || max_victims == 0) return 0;
  const size_t limit = max_victims > SIZE_MAX / 4 ? SIZE_MAX : 4 * max_victims;
  const uint32_t bands = opt_.lifetime_max_reads + 1;
  std::array<uint32_t, 64> candidates;
  std::array<uint32_t, 64> starts;
  std::array<bool, 64> done{};
  candidates.fill(kNoSlot);
  for (uint32_t band = 0; band < bands; ++band) {
    starts[band] = lifetime_queues_[band].cursor;
    done[band] = starts[band] == kNoSlot;
  }
  size_t visits = 0;
  size_t freed = 0;
  while (freed < max_victims && used_bytes_ > target_bytes && visits < limit) {
    if (!ReclaimAllowed()) break;
    uint32_t now = 0;
    if (!clock.TryDrainNow(&now)) break;
    uint32_t best = kNoSlot;
    uint32_t best_band = 0;
    int64_t best_score = 0;
    bool pending = false;
    uint32_t cached_pending = 0;
    for (uint32_t band = 0; band < bands; ++band)
      cached_pending += candidates[band] != kNoSlot;
    // Each nonempty band receives at most one resident check per round.
    // A pinned prefix cannot monopolize the tick before another band runs.
    for (uint32_t turn = 0; turn < bands && visits < limit; ++turn) {
      const uint32_t band = lifetime_next_band_;
      lifetime_next_band_ = (band + 1) % bands;
      if (done[band]) continue;
      LifetimeQueue& queue = lifetime_queues_[band];
      uint32_t current = candidates[band];
      if (current != kNoSlot) --cached_pending;
      if (current == kNoSlot) {
        current = queue.cursor;
        const uint32_t next = lifetime_links_[current].indexes[0].next;
        queue.cursor = next == kNoSlot ? queue.head : next;
        done[band] = queue.cursor == starts[band];
      }
      ++visits;
      ++watermark_scan_steps_;
      const SlotMeta& meta = slots_[current];
      if (meta.refs != 0 && stats != nullptr) ++stats->pin_skips;
      if (meta.live && !meta.remove_pending && meta.refs == 0 &&
          resident_generations_[current] != 0 &&
          resident_lifetime_states_[current].has_value() &&
          lifetime_links_[current].band == band) {
        candidates[band] = current;
        done[band] = false;
        const DiskLifetimeState& state = *resident_lifetime_states_[current];
        const int64_t score = DiskLifetimeScore(
            config, state.anchor, state.scoring_reads(),
            state.first_early_read(), state.write_armed(), now);
        // The lock excludes heat, pin, identity and membership mutations.
        // Rechecking a cached candidate still pays one visit this round.
        if (best == kNoSlot || score < best_score ||
            (score == best_score &&
             (state.anchor < resident_lifetime_states_[best]->anchor ||
              (state.anchor == resident_lifetime_states_[best]->anchor &&
               resident_generations_[current] <
                   resident_generations_[best])))) {
          best = current;
          best_band = band;
          best_score = score;
        }
      } else {
        candidates[band] = kNoSlot;
      }
      pending = pending || !done[band];
      // Leave one visit available when a checked winner already exists. This
      // also lets tiny caps make progress when they cannot cover every band.
      // Every candidate retained from the previous selection must compete
      // again first; a partial round must not discard a known colder one.
      if (best != kNoSlot && cached_pending == 0 && visits + 1 == limit) break;
    }
    // Do not start persistence after exhausting the shared access budget.
    if (visits == limit) {
      if (stats != nullptr) ++stats->budget_exhausted;
      break;
    }
    if (best == kNoSlot) {
      if (!pending) {
        if (stats != nullptr) ++stats->no_candidate;
        break;
      }
      continue;
    }
    if (!ReclaimAllowed() || !clock.TryDrainNow(&now)) break;
    // Unlike legacy's last-resident optimization, this path only clears one
    // slot record. No whole-extent donor operation belongs to watermarks.
    if (opt_.on_slot_evict) {
      if (!opt_.on_slot_evict(slots_[best].ref)) break;
    } else if (opt_.on_extent_evict) {
      break;  // A whole-extent callback cannot safely invalidate one slot.
    }
    const BlockKey victim = slots_[best].key;
    FreeSlotLocked(best);
    if (evicted != nullptr) evicted->push_back(victim);
    ++evictions_;
    ++watermark_evictions_;
    ++freed;
    if (stats != nullptr &&
        best_score > static_cast<int64_t>(config.keep_score_sec))
      ++stats->protected_victims;
    // Replenish only the winning band; every other cached candidate will be
    // checked against the next round's common clock before competing again.
    candidates[best_band] = kNoSlot;
    starts[best_band] = lifetime_queues_[best_band].cursor;
    done[best_band] = starts[best_band] == kNoSlot;
  }
  // An eligible resident observed on the last paid visit could not be
  // invalidated. Revisit it first next tick instead of wrapping through the
  // same pinned prefix forever. Nonwinning candidates must not be skipped.
  for (uint32_t band = 0; band < bands; ++band) {
    if (candidates[band] != kNoSlot)
      lifetime_queues_[band].cursor = candidates[band];
  }
  return freed;
}

size_t SlabAllocator::EvictColdToTarget(uint64_t target_bytes,
                                        size_t max_victims,
                                        std::vector<BlockKey>* evicted) {
  std::lock_guard<std::mutex> lock(mu_);
  const size_t limit =
      max_victims > std::numeric_limits<size_t>::max() / 4
          ? std::numeric_limits<size_t>::max()
          : 4 * max_victims;
  size_t freed = 0;
  for (size_t steps = 0;
       steps < limit && freed < max_victims && used_bytes_ > target_bytes &&
       global_hand_ != kNoSlot;
       ++steps) {
    if (!ReclaimAllowed()) break;
    ++watermark_scan_steps_;
    const uint32_t index = global_hand_;
    SlotMeta& meta = slots_[index];
    const uint32_t next =
        meta.global_next != kNoSlot ? meta.global_next : global_ring_head_;
    if (meta.refs > 0) {
      global_hand_ = next;
      continue;
    }
    // Actual GETs and new admissions already refresh global sweep order.
    // Leave reference credit to class-local CLOCK; it must not postpone an
    // older resident past more recent reads or writes here.

    if (!ReclaimAllowed()) break;
    const ExtentMeta& extent = extents_[meta.ref.extent];
    if (extent.resident_count == 1 && opt_.on_extent_evict) {
      if (!opt_.on_extent_evict(meta.ref.extent, extent.total_slots, 1))
        break;
    } else if (opt_.on_slot_evict) {
      if (!opt_.on_slot_evict(meta.ref)) break;
    } else if (opt_.on_extent_evict) {
      // A whole-extent callback cannot invalidate only this resident safely.
      break;
    }
    const BlockKey victim = meta.key;
    FreeSlotLocked(index);
    if (evicted) evicted->push_back(victim);
    ++evictions_;
    ++watermark_evictions_;
    ++freed;
  }
  return freed;
}

size_t SlabAllocator::Count() const {
  std::lock_guard<std::mutex> lock(mu_);
  return index_.size();
}
uint64_t SlabAllocator::UsedBytes() const {
  std::lock_guard<std::mutex> lock(mu_);
  return used_bytes_;
}
uint64_t SlabAllocator::Capacity() const {
  return static_cast<uint64_t>(opt_.num_extents) * opt_.extent_bytes;
}
uint64_t SlabAllocator::Evictions() const {
  std::lock_guard<std::mutex> lock(mu_);
  return evictions_;
}
uint64_t SlabAllocator::Steals() const {
  std::lock_guard<std::mutex> lock(mu_);
  return steals_;
}
uint64_t SlabAllocator::ColdSteals() const {
  std::lock_guard<std::mutex> lock(mu_);
  return cold_steals_;
}
SlabAllocator::LifetimeDonorStats SlabAllocator::DonorStats() const {
  const std::lock_guard<std::mutex> lock(mu_);
  return lifetime_donor_stats_;
}

SlabAllocator::LifetimeBucketMemory SlabAllocator::BucketMemory() const {
  const LifetimeMemory memory = LifetimeMemoryUsage();
  return {memory.entries, memory.bucket_bytes, memory.token_bytes};
}

SlabAllocator::LifetimeMemory SlabAllocator::LifetimeMemoryUsage() const {
  const std::lock_guard<std::mutex> lock(mu_);
  LifetimeMemory memory;
  memory.entries = lifetime_buckets_.size();
  memory.bucket_bytes = lifetime_buckets_.capacity() * sizeof(LifetimeBucket);
  memory.token_bytes = lifetime_bucket_tokens_.capacity() * sizeof(uint64_t);
  memory.identity_bytes = resident_generations_.capacity() * sizeof(uint64_t);
  memory.state_bytes = resident_lifetime_states_.capacity() *
                       sizeof(std::optional<DiskLifetimeState>);
  memory.link_bytes = lifetime_links_.capacity() * sizeof(LifetimeLinks);
  memory.queue_bytes = lifetime_queues_.capacity() * sizeof(LifetimeQueue);
  memory.version_bytes = lifetime_versions_.capacity() * sizeof(uint64_t);
  return memory;
}

SlabAllocator::MetadataMemory SlabAllocator::MetadataMemoryUsage() const {
  const std::lock_guard<std::mutex> lock(mu_);
  return {slots_.capacity() * sizeof(SlotMeta),
          extents_.capacity() * sizeof(ExtentMeta),
          classes_.capacity() * sizeof(Class),
          index_.BackingBytes(),
          class_rotation_bytes_,
          class_free_slot_bytes_};
}

SlabAllocator::LifetimeBucketObservation SlabAllocator::ObserveLifetimeBuckets(
    const DiskLifetimeClock& clock) const {
  if (opt_.lifetime_bucket_count == 0) return {};
  const std::lock_guard<std::mutex> lock(mu_);
  LifetimeBucketObservation observation;
  observation.summary_bytes =
      lifetime_bucket_summary_.capacity() * sizeof(LifetimeBucket);
  uint32_t now = 0;
  if (!clock.TryDrainNow(&now)) return observation;
  observation.available = true;
  const uint64_t current_page = now / opt_.lifetime_bucket_sec;
  for (const LifetimeBucket& bucket : lifetime_bucket_summary_) {
    if (bucket.generation > current_page)
      observation.protected_estimate += bucket.count;
  }
  return observation;
}

std::vector<SlabAllocator::LifetimeQueueStat>
SlabAllocator::LifetimeQueueStats() const {
  if (!opt_.track_lifetime_state) return {};
  const std::lock_guard<std::mutex> lock(mu_);
  const size_t bands = opt_.lifetime_max_reads + 1;
  size_t active_rows = 1;  // Global rows remain visible even without residents.
  for (const Class& cls : classes_) {
    if (cls.resident_count != 0) ++active_rows;
  }
  std::vector<LifetimeQueueStat> stats;
  stats.reserve(active_rows * bands);
  for (size_t row = 0; row <= classes_.size(); ++row) {
    if (row != 0 && classes_[row - 1].resident_count == 0) continue;
    const uint32_t slot_size = row == 0 ? 0 : classes_[row - 1].slot_size;
    for (uint32_t band = 0; band < bands; ++band) {
      stats.push_back(
          {slot_size, band, lifetime_queues_[row * bands + band].residents});
    }
  }
  return stats;
}

uint64_t SlabAllocator::WatermarkEvictions() const {
  std::lock_guard<std::mutex> lock(mu_);
  return watermark_evictions_;
}
uint64_t SlabAllocator::WatermarkScanVisits() const {
  std::lock_guard<std::mutex> lock(mu_);
  return watermark_scan_steps_;
}
uint64_t SlabAllocator::ExtentReturns() const {
  std::lock_guard<std::mutex> lock(mu_);
  return extent_returns_;
}
size_t SlabAllocator::ClassCount() const {
  std::lock_guard<std::mutex> lock(mu_);
  return classes_.size();
}
uint32_t SlabAllocator::PoolExtents() const {
  std::lock_guard<std::mutex> lock(mu_);
  return unbound_;
}
uint32_t SlabAllocator::BoundExtents() const {
  std::lock_guard<std::mutex> lock(mu_);
  uint32_t count = 0;
  for (const ExtentMeta& extent : extents_)
    if (extent.cls != kUnbound) ++count;
  return count;
}

SlabAllocator::IndexStats SlabAllocator::IndexStatsForTest() const {
  std::lock_guard<std::mutex> lock(mu_);
  return index_.stats();
}

size_t SlabAllocator::IndexBucketForTest(const BlockKey& key) const {
  std::lock_guard<std::mutex> lock(mu_);
  return index_.BucketForTest(key);
}

}  // namespace dfkv
