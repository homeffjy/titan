#pragma once
#include "cloud/filename.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"

namespace rocksdb {
namespace titandb {
class TitanFileSystem : public FileSystem {
 public:
  static Status NewTitanFileSystem(
      const std::shared_ptr<FileSystem>& base_fs,
      const std::shared_ptr<CloudFileSystem>& cloud_fs,
      TitanFileSystem** result) {
    *result = new TitanFileSystem(base_fs, cloud_fs);
    return Status::OK();
  }
  ~TitanFileSystem() override = default;

  auto GetCloudFileSystem() -> std::shared_ptr<CloudFileSystem> {
    return cloud_fs_;
  }

  static const char* kClassName() { return "titan-router"; }
  const char* Name() const { return kClassName(); }

  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& options,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    return GetAppropriateFS(fname)->NewSequentialFile(fname, options, result,
                                                      dbg);
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    return GetAppropriateFS(fname)->NewRandomAccessFile(fname, options, result,
                                                        dbg);
  }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    return GetAppropriateFS(fname)->NewWritableFile(fname, options, result,
                                                    dbg);
  }

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override {
    return GetAppropriateFS(fname)->DeleteFile(fname, options, dbg);
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    return GetAppropriateFS("")->NewDirectory(name, io_opts, result, dbg);
  }

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override {
    return GetAppropriateFS(fname)->FileExists(fname, options, dbg);
  }

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override {
    return GetAppropriateFS("")->GetChildren(dir, options, result, dbg);
  }

  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override {
    return GetAppropriateFS("")->CreateDir(dirname, options, dbg);
  }

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override {
    return GetAppropriateFS("")->CreateDirIfMissing(dirname, options, dbg);
  }

  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override {
    return GetAppropriateFS("")->DeleteDir(dirname, options, dbg);
  }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    return GetAppropriateFS(fname)->GetFileSize(fname, options, file_size, dbg);
  }

  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override {
    return GetAppropriateFS(fname)->GetFileModificationTime(fname, options,
                                                            file_mtime, dbg);
  }

  IOStatus RenameFile(const std::string& src, const std::string& target,
                      const IOOptions& options, IODebugContext* dbg) override {
    return GetAppropriateFS(src)->RenameFile(src, target, options, dbg);
  }

  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override {
    return GetAppropriateFS(fname)->LockFile(fname, options, lock, dbg);
  }

  IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                      IODebugContext* dbg) override {
    // FJY: TODO: Check whether this is ok
    return GetAppropriateFS("")->UnlockFile(lock, options, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override {
    // FJY: TODO: Check whether this is ok
    return GetAppropriateFS("")->GetTestDirectory(options, path, dbg);
  }

  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override {
    return GetAppropriateFS("")->GetAbsolutePath(db_path, options, output_path,
                                                 dbg);
  }

  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override {
    return GetAppropriateFS("")->IsDirectory(path, options, is_dir, dbg);
  }

 private:
  TitanFileSystem(const std::shared_ptr<FileSystem>& base_fs,
                  const std::shared_ptr<CloudFileSystem>& cloud_fs)
      : base_fs_(base_fs), cloud_fs_(cloud_fs) {};

  std::shared_ptr<FileSystem> base_fs_;
  std::shared_ptr<CloudFileSystem> cloud_fs_;

  auto GetAppropriateFS(const std::string& fname)
      -> std::shared_ptr<FileSystem> {
    return IsTitanFile(fname) ? cloud_fs_ : base_fs_;
  }

  static bool IsTitanFile(const std::string& fname) {
    return not Slice(fname).ends_with("sst");
  }
};

}  // namespace titandb
}  // namespace rocksdb
