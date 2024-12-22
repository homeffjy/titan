#include "blob_file_system.h"

namespace rocksdb {
namespace titandb {
Status TitanRouterFileSystem::NewTitanRouterFileSystem(
    const std::shared_ptr<FileSystem> &base_fs, const Options &options,
    const std::shared_ptr<Logger> &info_log,
    std::unique_ptr<TitanRouterFileSystem> *result) {
  result->reset(new TitanRouterFileSystem(base_fs, options, info_log));
  return Status::OK();
}

TitanRouterFileSystem::TitanRouterFileSystem(
    const std::shared_ptr<FileSystem> &base_fs, const Options &options,
    const std::shared_ptr<Logger> &info_log)
    : CloudFileSystemImpl(options, base_fs, info_log),
      blob_fs_(options.blob_fs) {
  assert(blob_fs_ != nullptr);
}

TitanRouterFileSystem::~TitanRouterFileSystem() = default;

bool TitanRouterFileSystem::IsBlobFile(const std::string &fname) {
  return Slice(fname).ends_with(".blob");
}

std::shared_ptr<FileSystem> TitanRouterFileSystem::GetFileSystem(
    const std::string &fname) const {
  return IsBlobFile(fname) ? blob_fs_ : GetBaseFileSystem();
}
}  // namespace titandb
}  // namespace rocksdb
