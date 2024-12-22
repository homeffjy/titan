#pragma once
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"

namespace rocksdb {
namespace titandb {
class TitanRouterFileSystem : public CloudFileSystemImpl {
  template <typename FileType, typename Func>
  IOStatus RouteFileOp(const std::string &fname, const FileOptions &options,
                       std::unique_ptr<FileType> *result, IODebugContext *dbg,
                       Func file_op) {
    auto fs = IsBlobFile(fname) ? blob_fs_ : GetBaseFileSystem();
    return (fs.get()->*file_op)(fname, options, result, dbg);
  }

  template <typename Func>
  IOStatus RouteSimpleOp(const std::string &fname, const IOOptions &options,
                         IODebugContext *dbg, Func file_op) {
    auto fs = IsBlobFile(fname) ? blob_fs_ : GetBaseFileSystem();
    return (fs.get()->*file_op)(fname, options, dbg);
  }

 public:
  struct Options : public CloudFileSystemOptions {
    std::shared_ptr<FileSystem> blob_fs;
    std::string db_path;
  };

  static Status NewTitanRouterFileSystem(
      const std::shared_ptr<FileSystem> &base_fs, const Options &options,
      const std::shared_ptr<Logger> &info_log,
      std::unique_ptr<TitanRouterFileSystem> *result);

  ~TitanRouterFileSystem() override;

  static const char *kClassName() { return "titan-router"; }
  const char *Name() const override { return kClassName(); }

  // Override necessary methods to route blob files to blob_fs_
  IOStatus NewSequentialFile(const std::string &fname,
                             const FileOptions &options,
                             std::unique_ptr<FSSequentialFile> *result,
                             IODebugContext *dbg) override {
    return RouteFileOp<FSSequentialFile>(fname, options, result, dbg,
                                         &FileSystem::NewSequentialFile);
  }

  IOStatus NewRandomAccessFile(const std::string &fname,
                               const FileOptions &options,
                               std::unique_ptr<FSRandomAccessFile> *result,
                               IODebugContext *dbg) override {
    return RouteFileOp<FSRandomAccessFile>(fname, options, result, dbg,
                                           &FileSystem::NewRandomAccessFile);
  }

  IOStatus NewWritableFile(const std::string &fname, const FileOptions &options,
                           std::unique_ptr<FSWritableFile> *result,
                           IODebugContext *dbg) override {
    return RouteFileOp<FSWritableFile>(fname, options, result, dbg,
                                       &FileSystem::NewWritableFile);
  }

  IOStatus DeleteFile(const std::string &fname, const IOOptions &options,
                      IODebugContext *dbg) override {
    return RouteSimpleOp(fname, options, dbg, &FileSystem::DeleteFile);
  }

  // Get underlying filesystem for blob files
  std::shared_ptr<FileSystem> GetBlobFileSystem() const { return blob_fs_; }

 private:
  TitanRouterFileSystem(const std::shared_ptr<FileSystem> &base_fs,
                        const Options &options,
                        const std::shared_ptr<Logger> &info_log);

  std::shared_ptr<FileSystem> blob_fs_;

  static bool IsBlobFile(const std::string &fname);

  std::shared_ptr<FileSystem> GetFileSystem(const std::string &fname) const;
};
}  // namespace titandb
}  // namespace rocksdb
