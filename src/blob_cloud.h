#pragma once
#include "rocksdb/cloud/cloud_file_system.h"
#include "titan/db.h"
#include "titan/options.h"

namespace rocksdb {
namespace titandb {

struct TitanCloudOptions {
  bool enabled = false;
  bool validate_filesize = true;
  bool roll_cloud_manifest_on_open = false;
  size_t max_manifest_file_size = 0;
};

class TitanCloudHelper {
 public:
  static Status InitializeCloudFS(TitanOptions& options,
                                  const std::string& dbname, bool read_only,
                                  bool* new_db);

  static Status FinalizeCloudDB(const TitanOptions& options,
                                const std::string& dbname, bool new_db,
                                const TitanDB* db);

 private:
  static Status SetupCloudManifest(const std::shared_ptr<CloudFileSystem>& cfs,
                                   const TitanOptions& options,
                                   const std::string& dbname, bool read_only,
                                   bool* new_db);
};
}  // namespace titandb
}  // namespace rocksdb