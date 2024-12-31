#pragma once
#include "rocksdb/cloud/cloud_file_system.h"
#include "titan/db.h"
#include "titan/options.h"

namespace rocksdb {
namespace titandb {
class TitanCloudHelper {
 public:
  static Status InitializeCloudFS(TitanOptions& options,
                                  const std::string& dbname,
                                  const std::string& persisten_cache_path,
                                  uint64_t persistent_cache_size_gb,
                                  bool read_only, bool* new_db);

  static Status FinalizeCloudDB(const TitanOptions& options,
                                const std::string& dbname, bool new_db,
                                const TitanDB* db);

 private:
  static Status SetupCloudManifest(const std::shared_ptr<CloudFileSystem>& cfs,
                                   const TitanOptions& options,
                                   const std::string& dbname, bool read_only,
                                   bool* new_db);

  static Status ConfigurePersistentCache(
      TitanOptions& options, const std::string& persistent_cache_path,
      uint64_t& persistent_cache_size_gb, std::unique_ptr<Env>& local_env);
};
}  // namespace titandb
}  // namespace rocksdb