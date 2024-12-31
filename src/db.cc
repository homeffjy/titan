#include "titan/db.h"

#include <blob_cloud.h>

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
  TitanOptions options = opt;

  // Created logger if it is not already pre-created by user.
  if (!options.info_log) {
    CreateLoggerFromOptions(dbname, options, &options.info_log);
  }

  bool new_db = false;
  Status st =
      TitanCloudHelper::InitializeCloudFS(options, dbname, read_only, &new_db);
  if (!st.ok()) {
    return st;
  }

  // FJY: TODO: Maybe remove persistent Cache
  assert(persistent_cache_path == "" && persistent_cache_size_gb == 0);

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

  st = TitanCloudHelper::FinalizeCloudDB(options, dbname, new_db, *db);
  if (!st.ok()) {
    delete *db;
    *db = nullptr;
  }

  return st;
}

}  // namespace titandb
}  // namespace rocksdb
