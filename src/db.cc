#include "titan/db.h"

#include "blob_cloud.h"
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

Status TitanDB::OpenWithCloud(const TitanOptions& options,
                              const std::string& dbname, TitanDB** db,
                              bool read_only) {
  TitanDBOptions db_options(options);
  TitanCFOptions cf_options(options);
  std::vector<TitanCFDescriptor> descs;
  descs.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;
  auto s = OpenWithCloud(options, dbname, descs, &handles, db, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // DBImpl is always holding the default handle.
    delete handles[0];
  }
  return s;
}

Status TitanDB::OpenWithCloud(const TitanOptions& options,
                              const std::string& dbname,
                              const std::vector<TitanCFDescriptor>& descs,
                              std::vector<ColumnFamilyHandle*>* handles,
                              TitanDB** db, bool read_only) {
  bool new_db = false;
  Status s = TitanCloudHelper::InitializeCloudResources(options, dbname,
                                                        read_only, &new_db);
  if (!s.ok()) {
    return s;
  }

  TitanDBOptions db_options(options);
  auto impl = new TitanDBImpl(db_options, dbname);
  s = impl->Open(descs, handles);
  if (s.ok()) {
    *db = impl;
  } else {
    *db = nullptr;
    delete impl;
    return s;
  }

  s = TitanCloudHelper::FinalizeCloudSetup(options, dbname, new_db, *db);
  if (!s.ok()) {
    *db = nullptr;
    delete impl;
  }

  return s;
}

}  // namespace titandb
}  // namespace rocksdb
