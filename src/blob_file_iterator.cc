#include "blob_file_iterator.h"

#include "blob_file_reader.h"
#include "table/block_based/block_based_table_reader.h"
#include "util.h"
#include "util/crc32c.h"

namespace rocksdb {
namespace titandb {

BlobFileIterator::BlobFileIterator(
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_name,
    uint64_t file_size, const TitanCFOptions& titan_cf_options)
    : file_(std::move(file)),
      file_number_(file_name),
      file_size_(file_size),
      titan_cf_options_(titan_cf_options),
      prefetch_buffer_(std::make_unique<FilePrefetchBuffer>(
          kMinReadaheadSize, kMaxReadaheadSize)) {}

BlobFileIterator::~BlobFileIterator() {}

bool BlobFileIterator::Init() {
  Slice slice;
  char header_buf[BlobFileHeader::kV3EncodedLength];
  IOOptions io_options;
  io_options.rate_limiter_priority = Env::IOPriority::IO_LOW;
  status_ = file_->Read(io_options, 0, BlobFileHeader::kV3EncodedLength, &slice,
                        header_buf, nullptr /*aligned_buf*/);
  if (!status_.ok()) {
    return false;
  }
  BlobFileHeader blob_file_header;
  status_ = DecodeInto(slice, &blob_file_header, true /*ignore_extra_bytes*/);
  if (!status_.ok()) {
    return false;
  }

  header_size_ = blob_file_header.size();

  char footer_buf[BlobFileFooter::kEncodedLength];
  status_ = file_->Read(io_options, file_size_ - BlobFileFooter::kEncodedLength,
                        BlobFileFooter::kEncodedLength, &slice, footer_buf,
                        nullptr /*aligned_buf*/);
  if (!status_.ok()) return false;
  BlobFileFooter blob_file_footer;
  status_ = blob_file_footer.DecodeFrom(&slice);
  end_of_blob_record_ = file_size_ - BlobFileFooter::kEncodedLength;
  if (!blob_file_footer.meta_index_handle.IsNull()) {
    end_of_blob_record_ -= (blob_file_footer.meta_index_handle.size() +
                            BlockBasedTable::kBlockTrailerSize);
  }

  if (blob_file_header.flags & BlobFileHeader::kHasUncompressionDictionary) {
    status_ = InitUncompressionDict(blob_file_footer, file_.get(),
                                    &uncompression_dict_,
                                    titan_cf_options_.memory_allocator());
    if (!status_.ok()) {
      return false;
    }
    decoder_.SetUncompressionDict(uncompression_dict_.get());
    // the layout of blob file is like:
    // |  ....   |
    // | records |
    // | compression dict + kBlockTrailerSize(5) |
    // | metaindex block(40) + kBlockTrailerSize(5) |
    // | footer(kEncodedLength: 32) |
    end_of_blob_record_ -= (uncompression_dict_->GetRawDict().size() +
                            BlockBasedTable::kBlockTrailerSize);
  }

  block_size_ = blob_file_header.block_size;

  assert(end_of_blob_record_ > BlobFileHeader::kV1EncodedLength);
  init_ = true;
  return true;
}

uint64_t BlobFileIterator::AdjustOffsetToNextBlockHead() {
  if (block_size_ == 0) return 0;
  uint64_t block_offset = iterate_offset_ % block_size_;
  if (block_offset != 0) {
    uint64_t shift = block_size_ - block_offset;
    iterate_offset_ += shift;
    return shift;
  }
  return 0;
}

void BlobFileIterator::SeekToFirst() {
  if (!init_ && !Init()) return;
  status_ = Status::OK();
  iterate_offset_ = header_size_;
  if (block_size_ != 0) {
    AdjustOffsetToNextBlockHead();
  }
  PrefetchAndGet();
}

bool BlobFileIterator::Valid() const { return valid_ && status().ok(); }

void BlobFileIterator::Next() {
  assert(init_);
  PrefetchAndGet();
}

Slice BlobFileIterator::key() const { return cur_blob_record_.key; }

Slice BlobFileIterator::value() const { return cur_blob_record_.value; }

void BlobFileIterator::IterateForPrev(uint64_t offset) {
  if (!init_ && !Init()) return;

  status_ = Status::OK();

  if (offset >= end_of_blob_record_) {
    iterate_offset_ = offset;
    status_ = Status::InvalidArgument("Out of bound");
    return;
  }

  uint64_t total_length = 0;
  FixedSlice<kRecordHeaderSize> header_buffer;
  iterate_offset_ = header_size_;
  while (iterate_offset_ < offset) {
    IOOptions io_options;
    // Since BlobFileIterator is only used for GC, we always set IO priority to
    // low.
    io_options.rate_limiter_priority = Env::IOPriority::IO_LOW;
    status_ = file_->Read(io_options, iterate_offset_, kRecordHeaderSize,
                          &header_buffer, header_buffer.get(),
                          nullptr /*aligned_buf*/);
    if (!status_.ok()) return;
    status_ = decoder_.DecodeHeader(&header_buffer);
    if (!status_.ok()) return;
    total_length = kRecordHeaderSize + decoder_.GetRecordSize();
    iterate_offset_ += total_length;
    if (block_size_ != 0) {
      total_length += AdjustOffsetToNextBlockHead();
    }
  }

  if (iterate_offset_ > offset) iterate_offset_ -= total_length;
  valid_ = false;
}

void BlobFileIterator::PrefetchAndGet() {
  FixedSlice<kRecordHeaderSize> header_buffer;
  // Since BlobFileIterator is only used for GC, we always set IO priority to
  // low.
  IOOptions io_options;
  io_options.rate_limiter_priority = Env::IOPriority::IO_LOW;
  bool prefetched = prefetch_buffer_->TryReadFromCache(
      io_options, file_.get(), iterate_offset_, kRecordHeaderSize,
      &header_buffer, &status_);
  if (!status_.ok()) return;
  if (!prefetched) {
    status_ = file_->Read(io_options, iterate_offset_, kRecordHeaderSize,
                          &header_buffer, header_buffer.get(),
                          nullptr /*aligned_buf*/);
  }
  if (!status_.ok()) return;
  status_ = decoder_.DecodeHeader(&header_buffer);
  if (!status_.ok()) return;

  Slice record_slice;
  auto record_size = decoder_.GetRecordSize();
  buffer_.resize(record_size);
  prefetched = prefetch_buffer_->TryReadFromCache(
      io_options, file_.get(), iterate_offset_ + kRecordHeaderSize, record_size,
      &record_slice, &status_);
  if (!prefetched) {
    status_ = file_->Read(io_options, iterate_offset_ + kRecordHeaderSize,
                          record_size, &record_slice, buffer_.data(),
                          nullptr /*aligned_buf*/);
  }
  if (status_.ok()) {
    status_ =
        decoder_.DecodeRecord(&record_slice, &cur_blob_record_, &uncompressed_,
                              titan_cf_options_.memory_allocator());
  }
  if (!status_.ok()) return;

  cur_record_offset_ = iterate_offset_;
  cur_record_size_ = kRecordHeaderSize + record_size;
  iterate_offset_ += cur_record_size_;
  AdjustOffsetToNextBlockHead();
  valid_ = true;
}

BlobFileMergeIterator::BlobFileMergeIterator(
    std::vector<std::unique_ptr<BlobFileIterator>>&& blob_file_iterators,
    const Comparator* comparator)
    : blob_file_iterators_(std::move(blob_file_iterators)),
      min_heap_(BlobFileIterComparator(comparator)) {}

bool BlobFileMergeIterator::Valid() const {
  if (current_ == nullptr) return false;
  if (!status().ok()) return false;
  return current_->Valid() && current_->status().ok();
}

void BlobFileMergeIterator::SeekToFirst() {
  for (auto& iter : blob_file_iterators_) {
    iter->SeekToFirst();
    if (iter->status().ok() && iter->Valid()) min_heap_.push(iter.get());
  }
  if (!min_heap_.empty()) {
    current_ = min_heap_.top();
    min_heap_.pop();
  } else {
    status_ = Status::Aborted("No iterator is valid");
  }
}

void BlobFileMergeIterator::Next() {
  assert(Valid());
  current_->Next();
  if (current_->status().ok() && current_->Valid()) min_heap_.push(current_);
  if (!min_heap_.empty()) {
    current_ = min_heap_.top();
    min_heap_.pop();
  } else {
    current_ = nullptr;
  }
}

Slice BlobFileMergeIterator::key() const {
  assert(current_ != nullptr);
  return current_->key();
}

Slice BlobFileMergeIterator::value() const {
  assert(current_ != nullptr);
  return current_->value();
}

}  // namespace titandb
}  // namespace rocksdb
