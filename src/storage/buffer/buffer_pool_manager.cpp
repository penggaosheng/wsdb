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
#include "buffer_pool_manager.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"

namespace wsdb {
#include <iostream>
using std::cerr, std::endl, std::ostream;
// #define LOG
#ifdef LOG
#define log cerr << "[log]:In " << __func__ << "line: " << __LINE__ << " "
#define TRACE_TRAP cerr << "[TRACER]:In " << __func__ << " hit " << __LINE__ << endl
#else
#define log  \
  if (false) \
  cerr
#define TRACE_TRAP (void)(0)
#endif

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, wsdb::LogManager *log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
{
  if (REPLACER == "LRUReplacer") {
    replacer_ = std::make_unique<LRUReplacer>();
  } else if (REPLACER == "LRUKReplacer") {
    replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
  } else {
    WSDB_FETAL("Unknown replacer: " + REPLACER);
  }
  // init free_list_
  for (frame_id_t i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); i++) {
    free_list_.push_back(i);
  }
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page *
{
  // Step 1.grant the latch
  std::lock_guard<std::mutex> lock(latch_);
  TRACE_TRAP;
  Frame *frame = GetFrame(fid, pid);
  if (frame != nullptr) {
    // Case 4. else pin the frame both in the buffer and the replacer and return the page
    frame->Pin();
    replacer_->Pin(page_frame_lookup_[{fid, pid}]);
    log << "fetch page: " << fid << " " << pid << "from buffer" << endl;
    return frame->GetPage();
  } else {
    // Case 3.if the page is not in the frame, GetAvailableFrame and UpdateFrame
    log << "fetch page: " << fid << " " << pid << "from disk" << endl;
    frame_id_t frame_id = GetAvailableFrame();
    UpdateFrame(frame_id, fid, pid);
    Page *page = frames_[frame_id].GetPage();
    disk_manager_->ReadPage(fid, pid, page->GetData());
    return page;
  }
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool
{
  // Step 1. grant the latch
  std::lock_guard<std::mutex> lock(latch_);
  const auto                  it = page_frame_lookup_.find({fid, pid});
  TRACE_TRAP;
  if (it == page_frame_lookup_.end() || !frames_[it->second].InUse()) {
    // Step 2. if the frame is not in the buffer or the frame is not in use, return false
    return false;
  }

  // Step 3. unpin the frame, after that if the frame is not in use, unpin the frame in the replacer
  Frame     *frame    = &frames_[it->second];
  frame_id_t frame_id = it->second;
  frame->Unpin();
  if (!frame->InUse()) {
    replacer_->Unpin(frame_id);
  }
  // Step 4. set the frame dirty if the page is dirty
  frame->SetDirty(is_dirty || frame->IsDirty());
  log << " Success unpined page:" << fid << " " << pid << " now the frame's pin count: " << frame->GetPinCount()
      << "in use :" << frame->InUse() << endl;
  return true;
}

auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool
{
  // Step 1. grant the latch
  std::lock_guard<std::mutex> lock(latch_);
  TRACE_TRAP;

  const auto it = page_frame_lookup_.find({fid, pid});
  if (it == page_frame_lookup_.end()) {
    // 2. if the page is not in the buffer, return true
    return true;
  }
  Frame     *frame    = &frames_[it->second];
  frame_id_t frame_id = it->second;
  if (frame->InUse()) {
    // 3. if the page is in use, return false
    return false;
  }
  // 4. flush the page to disk, reset the frame, add the frame to the free list and unpin the frame in the replacer
  Page *page = frame->GetPage();
  disk_manager_->WritePage(fid, pid, page->GetData());
  frame->Reset();
  free_list_.push_back(frame_id);
  replacer_->Unpin(frame_id);
  log << "delete page: " << fid << " " << pid << endl;
  // 5. update the page_frame_lookup_
  page_frame_lookup_.erase(it);
  return true;
}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool
{
  std::lock_guard<std::mutex> lock(latch_);
  TRACE_TRAP;
  for (auto it = page_frame_lookup_.begin(); it != page_frame_lookup_.end();) {
    if (it->first.fid == fid) {
      Frame     *frame    = &frames_[it->second];
      frame_id_t frame_id = it->second;
      if (frame->InUse()) {
        log << "fail to delete page for in use: " << it->first.fid << " " << it->first.pid << endl;
      }
      log << "deleting page: " << it->first.fid << " " << it->first.pid << endl;
      Page *page = frame->GetPage();
      disk_manager_->WritePage(fid, it->first.pid, page->GetData());
      frame->Reset();
      free_list_.push_back(frame_id);
      replacer_->Unpin(frame_id);
      it = page_frame_lookup_.erase(it);
    } else {
      it++;
    }
  }
  return true;
}

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool
{
  // 1. grant the latch
  std::lock_guard<std::mutex> lock(latch_);
  TRACE_TRAP;
  const auto it = page_frame_lookup_.find({fid, pid});
  if (it == page_frame_lookup_.end()) {
    // 2. if the page is not in the buffer, return false
    return false;
  }
  Frame *frame = &frames_[it->second];
  if (frame->IsDirty()) {
    // 3. flush the page to disk if the page is dirty
    Page *page = frame->GetPage();
    disk_manager_->WritePage(fid, pid, page->GetData());
    frame->SetDirty(false);
  }
  return true;
}

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool
{
  std::lock_guard<std::mutex> lock(latch_);
  TRACE_TRAP;
  for (const auto &it : page_frame_lookup_) {
    if (it.first.fid == fid) {
      Frame *frame = &frames_[it.second];
      if (frame->IsDirty()) {
        Page *page = frame->GetPage();
        disk_manager_->WritePage(fid, it.first.pid, page->GetData());
        frame->SetDirty(false);
      }
    }
  }
  return true;
}

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t
{
  if (!free_list_.empty()) {
    // 1. if the free list is not empty, get the frame id from the free list
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    log << "get frame: " << frame_id << " from free list" << endl;
    return frame_id;
  } else {
    // 2. else use the replacer to get the frame id
    frame_id_t frame_id;
    bool       found = replacer_->Victim(&frame_id);
    if (found) {
      log << "get frame: " << frame_id << " from replacer" << endl;
      return frame_id;
    } else {
      // 3. if no frame can be evicted, throw WSDB_NO_FREE_FRAME
      WSDB_THROW(WSDB_NO_FREE_FRAME, "No free frame");
    }
  }
}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid)
{
  Frame *frame      = &frames_[frame_id];
  auto   origin_fid = frame->GetPage()->GetFileId();
  auto   origin_pid = frame->GetPage()->GetPageId();
  if (frame->IsDirty()) {
    Page *page = frame->GetPage();
    disk_manager_->WritePage(origin_fid, origin_pid, page->GetData());
  }
  auto it = page_frame_lookup_.find({origin_fid, origin_pid});
  if (it != page_frame_lookup_.end()) {
    page_frame_lookup_.erase(it);
  }

  frame->Reset();
  frame->GetPage()->SetFilePageId(fid, pid);
  frame->Pin();
  replacer_->Pin(frame_id);
  page_frame_lookup_[{fid, pid}] = frame_id;
}

auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame *
{
  const auto it = page_frame_lookup_.find({fid, pid});
  return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
}

}  // namespace wsdb
