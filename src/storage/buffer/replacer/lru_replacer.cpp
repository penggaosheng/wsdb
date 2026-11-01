/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/17.
//

#include "lru_replacer.h"
#include <iostream>
#include "common/config.h"
#include "../common/error.h"
namespace wsdb {

// #define TRACE_F
#ifdef TRACE_F
#define TRACE_ENTER std::cerr << "[TRACE_ENTER]Enter " << __func__ << std::endl;
#define TRACE_EXIT std::cerr << "[TRACE_EXIT]Exit " << __func__ << std::endl;
#define TRACE_TRAP std::cerr << "[TRACE_TRAP]Trap " << __func__ << " Line: " << __LINE__ << std::endl;
#else
#define TRACE_ENTER (void)(0);
#define TRACE_EXIT (void)(0);
#define TRACE_TRAP (void)(0);
#endif

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool
{
  TRACE_ENTER
  std::lock_guard<std::mutex> lock(latch_);
  for (auto rit = lru_list_.rbegin(); rit != lru_list_.rend(); rit++) {
    if (rit->second) {
      *frame_id = rit->first;
      //   std::cerr << "Vicrim frame id is: " << *frame_id << std::endl;
      TRACE_TRAP
      lru_list_.erase(std::next(rit).base());
      TRACE_TRAP
      lru_hash_.erase(lru_hash_.find(*frame_id));
      cur_size_--;
      return true;
    }
  }
  TRACE_EXIT
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id)
{
  TRACE_ENTER
  //   std::cerr << "Pin: " << frame_id << std::endl;
  std::lock_guard<std::mutex> lock(latch_);
  auto                        hash_it   = lru_hash_.find(frame_id);
  bool                        is_inlist = hash_it != lru_hash_.end();
  if (!is_inlist) {
    if (lru_list_.size() < max_size_) {
      lru_list_.push_front(std::make_pair(frame_id, false));
      auto it = lru_list_.begin();
      lru_hash_.insert(std::make_pair(frame_id, it));
    }
  } else {
    auto list_it = hash_it->second;
    if (list_it->second) {
      cur_size_--;  // Only here need to decrease the size
    }
    lru_list_.erase(list_it);
    lru_list_.push_front(std::make_pair(frame_id, false));
    auto new_it     = lru_list_.begin();
    hash_it->second = new_it;
  }
  TRACE_EXIT
}

void LRUReplacer::Unpin(frame_id_t frame_id)
{
  TRACE_ENTER
  //   std::cerr << "Unpin: " << frame_id << std::endl;
  std::lock_guard<std::mutex> lock(latch_);
  auto                        hash_it = lru_hash_.find(frame_id);
  if (hash_it == lru_hash_.end()) {
    WSDB_THROW(WSDB_EXCEPTION_EMPTY, "frame not in the list");
  }
  auto list_it = hash_it->second;
  if (list_it->second) {
    // unpin a unpinned frame will not effect the size
    return;
  }
  list_it->second = true;
  cur_size_++;
  TRACE_EXIT
}

auto LRUReplacer::Size() -> size_t
{
  std::lock_guard<std::mutex> lock(latch_);
  return cur_size_;
}

}  // namespace wsdb
