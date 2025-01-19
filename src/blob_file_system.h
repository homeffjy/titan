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
  const char* Name() const override { return kClassName(); }

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
    return cloud_fs_->NewDirectory(name, io_opts, result, dbg);
  }

  IOStatus FileExists(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override {
    return GetAppropriateFS(fname)->FileExists(fname, options, dbg);
  }

  IOStatus GetChildren(const std::string& dir, const IOOptions& options,
                       std::vector<std::string>* result,
                       IODebugContext* dbg) override {
    // CloudFileSystem ListCloudObject not really using dir as path,
    // it gets titandb/*.blob even dir is titandb, so parse here
    std::vector<std::string> temp, titan_files, non_titan_files;
    auto status = cloud_fs_->GetChildren(dir, options, &temp, dbg);
    if (!status.ok()) {
      return status;
    }
    for (const auto& t : temp) {
      if (t.find(kTitanDB) != std::string::npos) {
        titan_files.push_back(t.substr(kTitanDB.size()));
      } else {
        non_titan_files.push_back(t);
      }
    }

    if (IsTitanFile(dir + pathsep)) {
      // CloudFileSystem doesn't know we keep titandb/MANIFEST in local
      status = base_fs_->GetChildren(dir, options, result, dbg);
      result->insert(result->end(), titan_files.begin(), titan_files.end());
    } else {
      *result = std::move(non_titan_files);
    }

    return status;
  }

  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override {
    return cloud_fs_->CreateDir(dirname, options, dbg);
  }

  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override {
    return cloud_fs_->CreateDirIfMissing(dirname, options, dbg);
  }

  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override {
    return cloud_fs_->DeleteDir(dirname, options, dbg);
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
    return cloud_fs_->UnlockFile(lock, options, dbg);
  }

  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override {
    return cloud_fs_->GetTestDirectory(options, path, dbg);
  }

  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override {
    return cloud_fs_->GetAbsolutePath(db_path, options, output_path, dbg);
  }

  IOStatus IsDirectory(const std::string& path, const IOOptions& options,
                       bool* is_dir, IODebugContext* dbg) override {
    return cloud_fs_->IsDirectory(path, options, is_dir, dbg);
  }

 private:
  TitanFileSystem(const std::shared_ptr<FileSystem>& base_fs,
                  const std::shared_ptr<CloudFileSystem>& cloud_fs)
      : base_fs_(base_fs), cloud_fs_(cloud_fs) {};

  std::shared_ptr<FileSystem> base_fs_;
  std::shared_ptr<CloudFileSystem> cloud_fs_;
  const std::string kTitanDB = "titandb/";

  auto GetAppropriateFS(const std::string& fname)
      -> std::shared_ptr<FileSystem> {
    return IsSstFile(fname) || (IsTitanFile(fname) && IsManifestFile(fname))
               ? base_fs_
               : cloud_fs_;
  }

  bool IsTitanFile(const std::string& fname) {
    size_t offset_1 = fname.find_last_of(pathsep);
    if (offset_1 != std::string::npos) {
      size_t offset_2 = fname.find_last_of(pathsep, offset_1 - 1);
      if (offset_2 != std::string::npos) {
        return fname.substr(offset_2 + 1, offset_1 - offset_2) == kTitanDB;
      }
    }
    return false;
  }
};

}  // namespace titandb
}  // namespace rocksdb
