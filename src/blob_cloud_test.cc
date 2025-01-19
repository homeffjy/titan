// Copyright (c) 2017 Rockset

#ifndef ROCKSDB_LITE

#ifdef USE_AWS

#include <aws/core/Aws.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <filesystem>

#include "blob_file_system.h"
#include "cloud/cloud_manifest.h"
#include "cloud/cloud_scheduler.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/cloud/cloud_file_deletion_scheduler.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "titan/db.h"
#include "util/random.h"
#include "util/string_util.h"
#ifndef OS_WIN
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {
namespace titandb {
namespace {
const FileOptions kFileOptions;
const IOOptions kIOOptions;
IODebugContext* const kDbg = nullptr;
}  // namespace

class BlobCloudTest : public testing::Test {
 public:
  BlobCloudTest() {
    Random64 rng(time(nullptr));
    test_id_ = std::to_string(rng.Next());
    fprintf(stderr, "Test ID: %s\n", test_id_.c_str());

    base_env_ = Env::Default();
    dbname_ = test::TmpDir() + "/blob_cloud-" + test_id_;
    clone_dir_ = test::TmpDir() + "/ctest-" + test_id_;
    cloud_fs_options_.TEST_Initialize("titan-test.", dbname_);
    cloud_fs_options_.use_aws_transfer_manager = true;
    // To catch any possible file deletion bugs, cloud files are deleted
    // right away
    cloud_fs_options_.cloud_file_deletion_delay = std::chrono::seconds(0);

    options_.dirname = dbname_ + "/titandb";
    options_.create_if_missing = true;
    options_.stats_dump_period_sec = 0;
    options_.stats_persist_period_sec = 0;
    options_.cloud_options.persistent_cache_path = "";
    options_.cloud_options.persistent_cache_size_gb = 0;
    db_ = nullptr;

    // Set min blob size to test
    options_.min_blob_size = 0;

    DestroyDir(dbname_);
    base_env_->CreateDirIfMissing(dbname_);
    base_env_->NewLogger(test::TmpDir(base_env_) + "/rocksdb-cloud.log",
                         &options_.info_log);
    options_.info_log->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);

    Cleanup();
  }

  void Cleanup() {
    ASSERT_TRUE(!aenv_);

    // check cloud credentials
    ASSERT_TRUE(cloud_fs_options_.credentials.HasValid().ok());

    CloudFileSystem* afs;
    // create a dummy aws env
    ASSERT_OK(CloudFileSystemEnv::NewAwsFileSystem(base_env_->GetFileSystem(),
                                                   cloud_fs_options_,
                                                   options_.info_log, &afs));
    ASSERT_NE(afs, nullptr);
    // delete all pre-existing contents from the bucket
    auto st = afs->GetStorageProvider()->EmptyBucket(afs->GetSrcBucketName(),
                                                     dbname_);
    delete afs;
    ASSERT_TRUE(st.ok() || st.IsNotFound());

    DestroyDir(clone_dir_);
    ASSERT_OK(base_env_->CreateDir(clone_dir_));
  }

  std::set<std::string> GetSSTFiles(std::string name) {
    std::vector<std::string> files;
    GetCloudFileSystem()->GetBaseFileSystem()->GetChildren(name, kIOOptions,
                                                           &files, kDbg);
    std::set<std::string> sst_files;
    for (auto& f : files) {
      if (IsSstFile(RemoveEpoch(f))) {
        sst_files.insert(f);
      }
    }
    return sst_files;
  }

  // Return total size of all sst files available locally
  void GetSSTFilesTotalSize(std::string name, uint64_t* total_size) {
    std::vector<std::string> files;
    GetCloudFileSystem()->GetBaseFileSystem()->GetChildren(name, kIOOptions,
                                                           &files, kDbg);
    std::set<std::string> sst_files;
    uint64_t local_size = 0;
    for (auto& f : files) {
      if (IsSstFile(RemoveEpoch(f))) {
        sst_files.insert(f);
        std::string lpath = dbname_ + "/" + f;
        ASSERT_OK(GetCloudFileSystem()->GetBaseFileSystem()->GetFileSize(
            lpath, kIOOptions, &local_size, kDbg));
        (*total_size) += local_size;
      }
    }
  }

  std::set<std::string> GetSSTFilesClone(std::string name) {
    std::string cname = clone_dir_ + "/" + name;
    return GetSSTFiles(cname);
  }

  void DestroyDir(const std::string& dir) {
    std::string cmd = "rm -rf " + dir;
    int rc = system(cmd.c_str());
    ASSERT_EQ(rc, 0);
  }

  virtual ~BlobCloudTest() {
    // Cleanup the cloud bucket
    if (!cloud_fs_options_.src_bucket.GetBucketName().empty()) {
      CloudFileSystem* afs;
      Status st = CloudFileSystemEnv::NewAwsFileSystem(
          base_env_->GetFileSystem(), cloud_fs_options_, options_.info_log,
          &afs);
      if (st.ok()) {
        afs->GetStorageProvider()->EmptyBucket(afs->GetSrcBucketName(),
                                               dbname_);
        delete afs;
      }
    }

    CloseDB();
  }

  void CreateCloudEnv() {
    CloudFileSystem* cfs;
    ASSERT_OK(CloudFileSystemEnv::NewAwsFileSystem(base_env_->GetFileSystem(),
                                                   cloud_fs_options_,
                                                   options_.info_log, &cfs));
    titandb::TitanFileSystem* tfs;
    auto t = std::shared_ptr<CloudFileSystem>(cfs);
    ASSERT_OK(titandb::TitanFileSystem::NewTitanFileSystem(
        base_env_->GetFileSystem(), t, &tfs));
    const std::shared_ptr<FileSystem> fs(tfs);
    aenv_ = CloudFileSystemEnv::NewCompositeEnv(base_env_, fs);
  }

  // Open database via the cloud interface
  void OpenDB() {
    std::vector<ColumnFamilyHandle*> handles;
    OpenDB(&handles);
    // Delete the handle for the default column family because the DBImpl
    // always holds a reference to it.
    ASSERT_TRUE(handles.size() > 0);
    delete handles[0];
  }

  // Open database via the cloud interface
  void OpenDB(std::vector<ColumnFamilyHandle*>* handles) {
    // default column family
    OpenWithColumnFamilies({kDefaultColumnFamilyName}, handles);
  }

  void OpenWithColumnFamilies(const std::vector<std::string>& cfs,
                              std::vector<ColumnFamilyHandle*>* handles) {
    ASSERT_TRUE(cloud_fs_options_.credentials.HasValid().ok());

    // Create new AWS env
    CreateCloudEnv();
    options_.env = aenv_.get();
    // Sleep for a second because S3 is eventual consistency.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_TRUE(db_ == nullptr);
    std::vector<TitanCFDescriptor> descs;
    for (size_t i = 0; i < cfs.size(); ++i) {
      descs.emplace_back(cfs[i], options_);
    }

    ASSERT_OK(titandb::TitanDB::OpenWithCloud(options_, dbname_, descs, handles,
                                              &db_));
    ASSERT_OK(db_->GetDbIdentity(dbid_));
  }

  // Try to open and return status
  Status checkOpen() {
    // Create new AWS env
    CreateCloudEnv();
    options_.env = aenv_.get();
    // Sleep for a second because S3 is eventual consistency.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    return TitanDB::OpenWithCloud(options_, dbname_, &db_);
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            std::vector<ColumnFamilyHandle*>* handles) {
    ASSERT_NE(db_, nullptr);
    size_t cfi = handles->size();
    handles->resize(cfi + cfs.size());
    for (auto cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(options_, cf, &handles->at(cfi++)));
    }
  }

  void CloseDB(std::vector<ColumnFamilyHandle*>* handles) {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    CloseDB();
  }

  void CloseDB() {
    if (db_) {
      db_->Flush(FlushOptions());  // convert pending writes to sst files
      delete db_;
      db_ = nullptr;
    }
  }

  void SetPersistentCache(const std::string& path, uint64_t size_gb) {
    options_.cloud_options.persistent_cache_path = path;
    options_.cloud_options.persistent_cache_size_gb = size_gb;
  }

  Status GetCloudLiveFilesSrc(std::set<uint64_t>* list) {
    auto* cfs = GetCloudFileSystem();
    std::unique_ptr<ManifestReader> manifest(
        new ManifestReader(options_.info_log, cfs, cfs->GetSrcBucketName()));
    return manifest->GetLiveFiles(cfs->GetSrcObjectPath(), list);
  }

  // Verify that local files are the same as cloud files in src bucket path
  void ValidateCloudLiveFilesSrcSize() {
    // Loop though all the files in the cloud manifest
    std::set<uint64_t> cloud_files;
    ASSERT_OK(GetCloudLiveFilesSrc(&cloud_files));
    for (uint64_t num : cloud_files) {
      std::string pathname = MakeTableFileName(dbname_, num);
      Log(options_.info_log, "cloud file list  %s\n", pathname.c_str());
    }

    std::set<std::string> localFiles = GetSSTFiles(dbname_);
    uint64_t cloudSize = 0;
    uint64_t localSize = 0;

    // loop through all the local files and validate
    for (std::string path : localFiles) {
      std::string cpath = GetCloudFileSystem()->GetSrcObjectPath() + "/" + path;
      ASSERT_OK(GetCloudFileSystem()->GetStorageProvider()->GetCloudObjectSize(
          GetCloudFileSystem()->GetSrcBucketName(), cpath, &cloudSize));

      // find the size of the file on local storage
      std::string lpath = dbname_ + "/" + path;
      ASSERT_OK(GetCloudFileSystem()->GetBaseFileSystem()->GetFileSize(
          lpath, kIOOptions, &localSize, kDbg));
      ASSERT_TRUE(localSize == cloudSize);
      Log(options_.info_log, "local file %s size %" PRIu64 "\n", lpath.c_str(),
          localSize);
      Log(options_.info_log, "cloud file %s size %" PRIu64 "\n", cpath.c_str(),
          cloudSize);
      printf("local file %s size %" PRIu64 "\n", lpath.c_str(), localSize);
      printf("cloud file %s size %" PRIu64 "\n", cpath.c_str(), cloudSize);
    }
  }

  CloudFileSystem* GetCloudFileSystem() const {
    EXPECT_TRUE(aenv_);
    return dynamic_cast<TitanFileSystem*>(aenv_->GetFileSystem().get())
        ->GetCloudFileSystem()
        .get();
  }
  CloudFileSystemImpl* GetCloudFileSystemImpl() const {
    EXPECT_TRUE(aenv_);
    return static_cast<CloudFileSystemImpl*>(
        dynamic_cast<TitanFileSystem*>(aenv_->GetFileSystem().get())
            ->GetCloudFileSystem()
            .get());
  }

  DBImpl* GetDBImpl() const { return static_cast<DBImpl*>(db_->GetBaseDB()); }

  Status SwitchToNewCookie(std::string new_cookie) {
    CloudManifestDelta delta{db_->GetNextFileNumber(), new_cookie};
    return ApplyCMDeltaToCloudDB(delta);
  }

  Status ApplyCMDeltaToCloudDB(const CloudManifestDelta& delta) {
    auto st = GetCloudFileSystem()->RollNewCookie(dbname_, delta.epoch, delta);
    if (!st.ok()) {
      return st;
    }
    bool applied = false;
    st = GetCloudFileSystem()->ApplyCloudManifestDelta(delta, &applied);
    assert(applied);
    if (!st.ok()) {
      return st;
    }
    db_->NewManifestOnNextUpdate();
    return st;
  }

 protected:
  void WaitUntilNoScheduledJobs() {
    while (true) {
      auto num = GetCloudFileSystemImpl()->TEST_NumScheduledJobs();
      if (num > 0) {
        usleep(100);
      } else {
        return;
      }
    }
  }

  std::vector<Env::FileAttributes> GetAllLocalFiles() {
    std::vector<Env::FileAttributes> local_files;
    assert(base_env_->GetChildrenFileAttributes(dbname_, &local_files).ok());
    return local_files;
  }

  // Generate a few obsolete sst files on an empty db
  static void GenerateObsoleteFilesOnEmptyDB(
      DBImpl* db, CloudFileSystem* cfs,
      std::vector<std::string>* obsolete_files) {
    ASSERT_OK(db->Put({}, "k1", "v1"));
    ASSERT_OK(db->Flush({}));

    ASSERT_OK(db->Put({}, "k1", "v2"));
    ASSERT_OK(db->Flush({}));

    std::vector<LiveFileMetaData> sst_files;
    db->GetLiveFilesMetaData(&sst_files);
    ASSERT_EQ(sst_files.size(), 2);
    for (auto& f : sst_files) {
      obsolete_files->push_back(cfs->RemapFilename(f.relative_filename));
    }

    // trigger compaction, so previous 2 sst files will be obsolete
    ASSERT_OK(db->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
    sst_files.clear();
    db->GetLiveFilesMetaData(&sst_files);
    ASSERT_EQ(sst_files.size(), 1);
  }

  // check that fname exists in in src bucket/object path
  rocksdb::Status ExistsCloudObject(const std::string& filename) const {
    return GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
        GetCloudFileSystem()->GetSrcBucketName(),
        GetCloudFileSystem()->GetSrcObjectPath() + pathsep + filename);
  }

  std::string test_id_;
  Env* base_env_;
  TitanOptions options_;
  std::string dbname_;
  std::string clone_dir_;
  CloudFileSystemOptions cloud_fs_options_;
  std::string dbid_;
  TitanDB* db_;
  std::unique_ptr<Env> aenv_;
};

//
// Most basic test. Create DB, write one key, close it and then check to see
// that the key exists.
//
TEST_F(BlobCloudTest, BasicTest) {
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);
  CloseDB();
  value.clear();

  // Reopen and validate
  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");

  std::set<uint64_t> live_files;
  ASSERT_OK(GetCloudLiveFilesSrc(&live_files));
  ASSERT_GT(live_files.size(), 0);
  CloseDB();
}

TEST_F(BlobCloudTest, FindAllLiveFilesTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // wait until files are persisted into s3
  GetDBImpl()->TEST_WaitForBackgroundWork();

  CloseDB();

  std::vector<std::string> tablefiles;
  std::string manifest;
  // fetch latest manifest to local
  ASSERT_OK(
      GetCloudFileSystem()->FindAllLiveFiles(dbname_, &tablefiles, &manifest));
  EXPECT_EQ(tablefiles.size(), 1);

  for (auto name : tablefiles) {
    EXPECT_EQ(GetFileType(name), RocksDBFileType::kSstFile);
    // verify that the sst file indeed NOT exists in cloud
    EXPECT_NOK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
        GetCloudFileSystem()->GetSrcBucketName(),
        GetCloudFileSystem()->GetSrcObjectPath() + pathsep + name));
  }

  EXPECT_EQ(GetFileType(manifest), RocksDBFileType::kManifestFile);
  // verify that manifest file indeed exists in cloud
  auto storage_provider = GetCloudFileSystem()->GetStorageProvider();
  auto bucket_name = GetCloudFileSystem()->GetSrcBucketName();
  auto object_path =
      GetCloudFileSystem()->GetSrcObjectPath() + pathsep + manifest;
  EXPECT_OK(storage_provider->ExistsCloudObject(bucket_name, object_path));
}

// Files of dropped CF should not be included in live files
TEST_F(BlobCloudTest, LiveFilesOfDroppedCFTest) {
  std::vector<ColumnFamilyHandle*> handles;
  OpenDB(&handles);

  std::vector<std::string> tablefiles;
  std::string manifest;
  ASSERT_OK(
      GetCloudFileSystem()->FindAllLiveFiles(dbname_, &tablefiles, &manifest));

  EXPECT_TRUE(tablefiles.empty());
  CreateColumnFamilies({"cf1"}, &handles);

  // write to CF
  ASSERT_OK(db_->Put(WriteOptions(), handles[1], "hello", "world"));
  // flush cf1
  ASSERT_OK(db_->Flush({}, handles[1]));

  tablefiles.clear();
  ASSERT_OK(
      GetCloudFileSystem()->FindAllLiveFiles(dbname_, &tablefiles, &manifest));
  EXPECT_TRUE(tablefiles.size() == 1);

  // Drop the CF
  ASSERT_OK(db_->DropColumnFamily(handles[1]));
  tablefiles.clear();
  // make sure that files are not listed as live for dropped CF
  ASSERT_OK(
      GetCloudFileSystem()->FindAllLiveFiles(dbname_, &tablefiles, &manifest));
  EXPECT_TRUE(tablefiles.empty());
  CloseDB(&handles);
}

// Verifies that when we move files across levels, the files are still listed as
// live files
TEST_F(BlobCloudTest, LiveFilesAfterChangingLevelTest) {
  options_.num_levels = 3;
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "a", "1"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "2"));
  ASSERT_OK(db_->Flush({}));
  auto db_impl = GetDBImpl();

  std::vector<std::string> tablefiles_before_move;
  std::string manifest;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(
      dbname_, &tablefiles_before_move, &manifest));
  EXPECT_EQ(tablefiles_before_move.size(), 1);

  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  // Move the sst files to another level by compacting entire range
  ASSERT_OK(db_->CompactRange(cro, nullptr /* begin */, nullptr /* end */));

  ASSERT_OK(db_impl->TEST_WaitForBackgroundWork());

  std::vector<std::string> tablefiles_after_move;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(
      dbname_, &tablefiles_after_move, &manifest));
  EXPECT_EQ(tablefiles_before_move, tablefiles_after_move);
}

TEST_F(BlobCloudTest, GetChildrenTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::vector<std::string> children;
  ASSERT_OK(aenv_->GetFileSystem()->GetChildren(options_.dirname, kIOOptions,
                                                &children, kDbg));
  int blob_files = 0;
  for (const auto& c : children) {
    if (IsBlobFile(c)) {
      blob_files++;
    }
  }
  EXPECT_EQ(blob_files, 1);

  for (auto& fname : children) {
    if (fname != "." && fname != "..") {
      ASSERT_OK(aenv_->DeleteFile(options_.dirname + "/" + fname));
    }
  }
  ASSERT_OK(aenv_->DeleteDir(options_.dirname));

  children.clear();
  ASSERT_OK(aenv_->GetFileSystem()->GetChildren(dbname_, kIOOptions, &children,
                                                kDbg));
  int sst_files = 0;
  for (const auto& c : children) {
    if (IsSstFile(c)) {
      sst_files++;
    }
  }
  EXPECT_EQ(sst_files, 1);

  for (auto& fname : children) {
    if (fname != "." && fname != "..") {
      ASSERT_OK(aenv_->DeleteFile(dbname_ + "/" + fname));
    }
  }
  // still has manifest with cloud file system
  ASSERT_NOK(aenv_->DeleteDir(dbname_));
}

TEST_F(BlobCloudTest, FindLiveFilesFromLocalManifestTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "Universe"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // wait until files are persisted into s3
  GetDBImpl()->TEST_WaitForBackgroundWork();

  CloseDB();

  // determine the manifest name and store a copy in a different location
  auto cfs = GetCloudFileSystem();
  auto manifest_file = cfs->RemapFilename("MANIFEST");
  auto manifest_path = std::filesystem::path(dbname_) / manifest_file;

  auto alt_manifest_path =
      std::filesystem::temp_directory_path() / ("ALT-" + manifest_file);
  std::filesystem::copy_file(manifest_path, alt_manifest_path);

  DestroyDir(dbname_);

  std::vector<std::string> tablefiles;
  // verify the copied manifest can be processed correctly
  ASSERT_OK(GetCloudFileSystem()->FindLiveFilesFromLocalManifest(
      alt_manifest_path, &tablefiles));

  // verify the result
  EXPECT_EQ(tablefiles.size(), 1);

  for (auto name : tablefiles) {
    EXPECT_EQ(GetFileType(name), RocksDBFileType::kSstFile);
    // verify that the sst file indeed not exists in cloud
    EXPECT_NOK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
        GetCloudFileSystem()->GetSrcBucketName(),
        GetCloudFileSystem()->GetSrcObjectPath() + pathsep + name));
  }

  // clean up
  std::filesystem::remove(alt_manifest_path);
}
//
// verify that dbid registry is appropriately handled
//
TEST_F(BlobCloudTest, DbidRegistry) {
  // Put one key-value
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_TRUE(value.compare("World") == 0);

  // Assert that there is one db in the registry
  DbidList dbs;
  ASSERT_OK(GetCloudFileSystem()->GetDbidList(
      GetCloudFileSystem()->GetSrcBucketName(), &dbs));
  ASSERT_GE(dbs.size(), 1);

  CloseDB();
}
}  // namespace titandb
}  // namespace ROCKSDB_NAMESPACE

// A black-box test for the cloud wrapper around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  Aws::InitAPI(Aws::SDKOptions());
  auto r = RUN_ALL_TESTS();
  Aws::ShutdownAPI(Aws::SDKOptions());
  return r;
}

#else  // USE_AWS

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr,
          "SKIPPED as TitanDB is supported only when USE_AWS is defined.\n");
  return 0;
}
#endif

#else  // ROCKSDB_LITE

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr, "SKIPPED as TitanDB is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
