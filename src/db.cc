#include "titan/db.h"

#include "blob_file_system.h"
#include "cloud/db_cloud_impl.h"
#include "db_impl.h"
#include "env/composite_env_wrapper.h"
#include "rocksdb/cloud/cloud_file_system.h"

namespace rocksdb {
namespace titandb {

Status TitanDB::Open(const TitanOptions& options, const std::string& dbname,
                     TitanDB** db) {
  TitanDBOptions db_options(options);
  TitanCFOptions cf_options(options);
  std::vector<TitanCFDescriptor> descs;
  descs.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;
  Status s = TitanDB::Open(db_options, dbname, descs, &handles, db);
  if (s.ok()) {
    assert(handles.size() == 1);
    // DBImpl is always holding the default handle.
    delete handles[0];
  }
  return s;
}

Status TitanDB::Open(const TitanDBOptions& db_options,
                     const std::string& dbname,
                     const std::vector<TitanCFDescriptor>& descs,
                     std::vector<ColumnFamilyHandle*>* handles, TitanDB** db) {
  auto impl = new TitanDBImpl(db_options, dbname);
  auto s = impl->Open(descs, handles);
  if (s.ok()) {
    *db = impl;
  } else {
    *db = nullptr;
    delete impl;
  }
  return s;
}

Status TitanDB::Open(const TitanOptions& opt, const std::string& dbname,
                     TitanDB** db, const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb, bool read_only) {
  Status st;
  TitanOptions options = opt;

  // Created logger if it is not already pre-created by user.
  if (!options.info_log) {
    CreateLoggerFromOptions(dbname, options, &options.info_log);
  }

  auto cfs = dynamic_cast<TitanFileSystem*>(options.env->GetFileSystem().get())
                  ->GetCloudFileSystem();
  assert(cfs);
  if (!cfs->GetLogger()) {
    cfs->SetLogger(options.info_log);
  }
  // FJY: TODO: Maybe remove constant sst file size

  const auto& local_fs = cfs->GetBaseFileSystem();
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  if (!read_only) {
    local_fs->CreateDirIfMissing(dbname, io_opts,
                                 dbg);  // MJR: TODO: Move into sanitize
  }

  bool new_db = false;
  // If cloud manifest is already loaded, this means the directory has been
  // sanitized (possibly by the call to ListColumnFamilies())
  if (cfs->GetCloudManifest() == nullptr) {
    st = cfs->SanitizeLocalDirectory(options, dbname, read_only);

    if (st.ok()) {
      st = cfs->LoadCloudManifest(dbname, read_only);
    }
    if (st.IsNotFound()) {
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "CLOUDMANIFEST not found in the cloud, assuming this is a new "
          "database");
      new_db = true;
      st = Status::OK();
    } else if (!st.ok()) {
      return st;
    }
  }
  if (new_db) {
    if (read_only || !options.create_if_missing) {
      return Status::NotFound(
          "CLOUDMANIFEST not found and not creating new db");
    }
    st = cfs->CreateCloudManifest(
        dbname, cfs->GetCloudFileSystemOptions().new_cookie_on_open);
    if (!st.ok()) {
      return st;
    }
  }

  // Local environment, to be owned by DBCloudImpl, so that it outlives the
  // cache object created below.
  std::unique_ptr<Env> local_env(
      new CompositeEnvWrapper(options.env, local_fs));

  // FJY: TODO: Maybe remove persistent Cache
  assert(persistent_cache_path == "" && persistent_cache_size_gb == 0);

  // We do not want a very large MANIFEST file because the MANIFEST file is
  // uploaded to S3 for every update, so always enable rolling of Manifest file
  options.max_manifest_file_size =
      4 * 1024L * 1024L;  // FJY: TODO: Move this constant

  std::string dbid;
  TitanDBOptions db_options(options);
  TitanCFOptions cf_options(options);
  std::vector<TitanCFDescriptor> descs;
  descs.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;
  st = Open(db_options, dbname, descs, &handles, db);
  if (st.ok()) {
    assert(handles.size() == 1);
    // DBImpl is always holding the default handle.
    delete handles[0];
  } else {
    return st;
  }

  (*db)->GetDbIdentity(dbid);

  if (new_db && st.ok() && cfs->HasDestBucket() &&
      cfs->GetCloudFileSystemOptions().roll_cloud_manifest_on_open) {
    // This is a new database, upload the CLOUDMANIFEST after all MANIFEST file
    // was already uploaded. It is at this point we consider the database
    // committed in the cloud.
    st = cfs->UploadCloudManifest(
        dbname, cfs->GetCloudFileSystemOptions().new_cookie_on_open);
  }

  // now that the database is opened, all file sizes have been verified and we
  // no longer need to verify file sizes for each file that we open. Note that
  // this might have a data race with background compaction, but it's not a big
  // deal, since it's a boolean and it does not impact correctness in any way.
  if (cfs->GetCloudFileSystemOptions().validate_filesize) {
    *const_cast<bool*>(&cfs->GetCloudFileSystemOptions().validate_filesize) =
        false;
  }

  Log(InfoLogLevel::INFO_LEVEL, options.info_log,
      "Opened cloud db with local dir %s dbid %s. %s", dbname.c_str(),
      dbid.c_str(), st.ToString().c_str());
  return st;
}

}  // namespace titandb
}  // namespace rocksdb
