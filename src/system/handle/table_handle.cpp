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
// Created by ziqi on 2024/7/19.
//

#include "table_handle.h"
namespace wsdb {

TableHandle::TableHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, table_id_t table_id,
    TableHeader &hdr, RecordSchemaUptr &schema, StorageModel storage_model)
    : tab_hdr_(hdr),
      table_id_(table_id),
      disk_manager_(disk_manager),
      buffer_pool_manager_(buffer_pool_manager),
      schema_(std::move(schema)),
      storage_model_(storage_model)
{
  // set table id for table handle;
  schema_->SetTableId(table_id_);
  if (storage_model_ == PAX_MODEL) {
    field_offset_.resize(schema_->GetFieldCount());
    // calculate offsets of fields
    WSDB_STUDENT_TODO(l1, f2);
  }
}

auto TableHandle::GetRecord(const RID &rid) -> RecordUptr
{
  auto nullmap = std::make_unique<char[]>(tab_hdr_.nullmap_size_);
  auto data    = std::make_unique<char[]>(tab_hdr_.rec_size_);

  // 1. fetch the page handle by rid
  auto page_id = rid.PageID();
  auto slot_id = rid.SlotID();
  auto pg_hdl  = FetchPageHandle(page_id);

  // 2. check if there is a record in the slot using bitmap, if not, unpin the page and throw WSDB_RECORD_MISS
  if (!BitMap::GetBit(pg_hdl->GetBitmap(), slot_id)) {
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    WSDB_THROW(WSDB_RECORD_MISS, "Record not found");
  }

  // 3. read the record from the slot using page handle
  pg_hdl->ReadSlot(slot_id, nullmap.get(), data.get());

  // 4. unpin the page
  buffer_pool_manager_->UnpinPage(table_id_, page_id, false);

  return std::make_unique<Record>(schema_.get(), nullmap.get(), data.get(), rid);
}

auto TableHandle::GetChunk(page_id_t pid, const RecordSchema *chunk_schema) -> ChunkUptr { WSDB_STUDENT_TODO(l1, f2); }

auto TableHandle::InsertRecord(const Record &record) -> RID
{
  // 1. create a page handle using CreatePageHandle
  auto pg_hdl = CreatePageHandle();

  // 2. get an empty slot in the page
  auto slot_id = BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, 0, false);
  RID ret_rid = {pg_hdl->GetPage()->GetPageId(), static_cast<slot_id_t>(slot_id)};

  // 3. write the record into the slot
  pg_hdl->WriteSlot(slot_id, record.GetNullMap(), record.GetData(), false);

  // 4. update the bitmap and the number of records in the page header
  BitMap::SetBit(pg_hdl->GetBitmap(), slot_id, true);
  tab_hdr_.rec_num_++;

  // 5. if the page is full after inserting the record, update the first free page id in the file header and set the
  // next page id of the current page
  if (BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, 0, false) == tab_hdr_.rec_per_page_) {
    // This indicates that the page is full
    tab_hdr_.first_free_page_ = pg_hdl->GetPage()->GetNextFreePageId();
    pg_hdl->GetPage()->SetNextFreePageId(INVALID_PAGE_ID);
  }

  // 6. unpin the page
  buffer_pool_manager_->UnpinPage(table_id_, pg_hdl->GetPage()->GetPageId(), true);

  return ret_rid;
}

void TableHandle::InsertRecord(const RID &rid, const Record &record)
{
  // 1. if rid is invalid, unpin the page and throw WSDB_PAGE_MISS
  if (rid.PageID() == INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(table_id_, rid.PageID(), false);
    WSDB_THROW(WSDB_PAGE_MISS, fmt::format("Page: {}", rid.PageID()));
  }

  // 2. fetch the page handle and check the bitmap, if the slot is not empty, throw WSDB_RECORD_EXISTS
  auto page_id = rid.PageID();
  auto slot_id = rid.SlotID();
  auto pg_hdl  = FetchPageHandle(rid.PageID());
  if (BitMap::GetBit(pg_hdl->GetBitmap(), slot_id)) {
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    WSDB_THROW(WSDB_RECORD_EXISTS, fmt::format("Record: {}", rid.SlotID()));
  }

  // 3. do the rest of the steps in InsertRecord 3-6

  // 3.3. write the record into the slot
  pg_hdl->WriteSlot(slot_id, record.GetNullMap(), record.GetData(), false);

  // 3.4. update the bitmap and the number of records in the page header
  BitMap::SetBit(pg_hdl->GetBitmap(), slot_id, true);
  tab_hdr_.rec_num_++;

  // 3.5. if the page is full after inserting the record, update the first free page id in the file header and set the
  // next page id of the current page
  if (BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, 0, false) == tab_hdr_.rec_per_page_) {
    // This indicates that the page is full
    tab_hdr_.first_free_page_ = pg_hdl->GetPage()->GetNextFreePageId();
    pg_hdl->GetPage()->SetNextFreePageId(INVALID_PAGE_ID);
  }

  // 3.6. unpin the page
  buffer_pool_manager_->UnpinPage(table_id_, pg_hdl->GetPage()->GetPageId(), true);
}

void TableHandle::DeleteRecord(const RID &rid)
{
  // 1. if the slot is empty, unpin the page and throw WSDB_RECORD_MISS
  auto page_id = rid.PageID();
  auto slot_id = rid.SlotID();
  auto pg_hdl  = FetchPageHandle(page_id);
  if (!BitMap::GetBit(pg_hdl->GetBitmap(), slot_id)) {
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    WSDB_THROW(WSDB_RECORD_MISS, fmt::format("Record: {}", slot_id));
  }

  // 2. update the bitmap and the number of records in the page header
  BitMap::SetBit(pg_hdl->GetBitmap(), slot_id, false);
  tab_hdr_.rec_num_--;

  // 3. if the page is not full after deleting the record, update the first free page id in the file header and the next
  // page id in the page header
  if (BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, 0, false) != tab_hdr_.rec_per_page_) {
    // This means the page is not full now
    pg_hdl->GetPage()->SetNextFreePageId(tab_hdr_.first_free_page_);
    tab_hdr_.first_free_page_ = page_id;
  }

  // 4. unpin the page
  buffer_pool_manager_->UnpinPage(table_id_, page_id, true);
}

void TableHandle::UpdateRecord(const RID &rid, const Record &record)
{
  // 1. if the slot is empty, unpin the page and throw WSDB_RECORD_MISS
  auto page_id = rid.PageID();
  auto slot_id = rid.SlotID();
  auto pg_hdl  = FetchPageHandle(page_id);
  if (!BitMap::GetBit(pg_hdl->GetBitmap(), slot_id)) {
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    WSDB_THROW(WSDB_RECORD_MISS, fmt::format("Record: {}", slot_id));
  }

  // 2. write slot
  pg_hdl->WriteSlot(slot_id, record.GetNullMap(), record.GetData(), true);

  // 3. unpin the page
  buffer_pool_manager_->UnpinPage(table_id_, page_id, true);
}

auto TableHandle::FetchPageHandle(page_id_t page_id) -> PageHandleUptr
{
  auto page = buffer_pool_manager_->FetchPage(table_id_, page_id);
  return WrapPageHandle(page);
}

auto TableHandle::CreatePageHandle() -> PageHandleUptr
{
  if (tab_hdr_.first_free_page_ == INVALID_PAGE_ID) {
    return CreateNewPageHandle();
  }
  auto page = buffer_pool_manager_->FetchPage(table_id_, tab_hdr_.first_free_page_);
  return WrapPageHandle(page);
}

auto TableHandle::CreateNewPageHandle() -> PageHandleUptr
{
  auto page_id = static_cast<page_id_t>(tab_hdr_.page_num_);
  tab_hdr_.page_num_++;
  auto page   = buffer_pool_manager_->FetchPage(table_id_, page_id);
  auto pg_hdl = WrapPageHandle(page);
  page->SetNextFreePageId(tab_hdr_.first_free_page_);
  tab_hdr_.first_free_page_ = page_id;
  return pg_hdl;
}

auto TableHandle::WrapPageHandle(Page *page) -> PageHandleUptr
{
  switch (storage_model_) {
    case StorageModel::NARY_MODEL: return std::make_unique<NAryPageHandle>(&tab_hdr_, page);
    case StorageModel::PAX_MODEL: return std::make_unique<PAXPageHandle>(&tab_hdr_, page, schema_.get(), field_offset_);
    default: WSDB_FETAL("Unknown storage model");
  }
}

auto TableHandle::GetTableId() const -> table_id_t { return table_id_; }

auto TableHandle::GetTableHeader() const -> const TableHeader & { return tab_hdr_; }

auto TableHandle::GetSchema() const -> const RecordSchema & { return *schema_; }

auto TableHandle::GetTableName() const -> std::string
{
  auto file_name = disk_manager_->GetFileName(table_id_);
  return OBJNAME_FROM_FILENAME(file_name);
}

auto TableHandle::GetStorageModel() const -> StorageModel { return storage_model_; }

auto TableHandle::GetFirstRID() -> RID
{
  auto page_id = FILE_HEADER_PAGE_ID + 1;
  while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
    auto pg_hdl = FetchPageHandle(page_id);
    auto id     = BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, 0, true);
    if (id != tab_hdr_.rec_per_page_) {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      return {page_id, static_cast<slot_id_t>(id)};
    }
    buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
    page_id++;
  }
  return INVALID_RID;
}

auto TableHandle::GetNextRID(const RID &rid) -> RID
{
  auto page_id = rid.PageID();
  auto slot_id = rid.SlotID();
  while (page_id < static_cast<page_id_t>(tab_hdr_.page_num_)) {
    auto pg_hdl = FetchPageHandle(page_id);
    slot_id = static_cast<slot_id_t>(BitMap::FindFirst(pg_hdl->GetBitmap(), tab_hdr_.rec_per_page_, slot_id + 1, true));
    if (slot_id == static_cast<slot_id_t>(tab_hdr_.rec_per_page_)) {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      page_id++;
      slot_id = -1;
    } else {
      buffer_pool_manager_->UnpinPage(table_id_, page_id, false);
      return {page_id, static_cast<slot_id_t>(slot_id)};
    }
  }
  return INVALID_RID;
}

auto TableHandle::HasField(const std::string &field_name) const -> bool
{
  return schema_->HasField(table_id_, field_name);
}

}  // namespace wsdb
