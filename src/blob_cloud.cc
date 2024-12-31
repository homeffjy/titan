#include "blob_cloud.h"

#include <env/composite_env_wrapper.h>

#include "blob_file_system.h"

namespace rocksdb {
namespace titandb {
Status TitanCloudHelper::InitializeCloudFS(TitanOptions& options,
                                           const std::string& dbname,
                                           bool read_only, bool* new_db) {
  Status st;

  auto cfs = dynamic_cast<TitanFileSystem*>(options.env->GetFileSystem().get())
                 ->GetCloudFileSystem();
  if (!cfs) {
    return Status::InvalidArgument("Cloud filesystem not properly initialized");
  }
  if (!cfs->GetLogger()) {
    cfs->SetLogger(options.info_log);
  }

  const auto& local_fs = cfs->GetBaseFileSystem();
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  if (!read_only) {
    st = local_fs->CreateDirIfMissing(dbname, io_opts, dbg);
    if (!st.ok()) return st;
  }

  st = SetupCloudManifest(cfs, options, dbname, read_only, new_db);

  // Local environment, to be owned by DBCloudImpl, so that it outlives the
  // cache object created below.
  std::unique_ptr<Env> local_env(
      new CompositeEnvWrapper(options.env, local_fs));

  // We do not want a very large MANIFEST file because the MANIFEST file is
  // uploaded to S3 for every update, so always enable rolling of Manifest file
  options.max_manifest_file_size =
      4 * 1024L * 1024L;  // FJY: TODO: Move this constant

  return st;
}

Status TitanCloudHelper::SetupCloudManifest(
    const std::shared_ptr<CloudFileSystem>& cfs, const TitanOptions& options,
    const std::string& dbname, const bool read_only, bool* new_db) {
  Status st;
  *new_db = false;
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
      *new_db = true;
      st = Status::OK();
    } else if (!st.ok()) {
      return st;
    }
  }
  if (*new_db) {
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
  return st;
}

Status TitanCloudHelper::FinalizeCloudDB(const TitanOptions& options,
                                         const std::string& dbname, bool new_db,
                                         const TitanDB* db) {
  Status st;
  auto cfs = dynamic_cast<TitanFileSystem*>(options.env->GetFileSystem().get())
                 ->GetCloudFileSystem();
  std::string dbid;
  db->GetDbIdentity(dbid);

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