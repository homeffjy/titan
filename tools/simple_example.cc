#include <aws/core/Aws.h>
#include <rocksdb/cloud/cloud_file_system.h>

#include <cassert>

#include "blob_file_system.h"
#include "titan/db.h"

// This is the local directory where the db is stored.
std::string kDBPath = "/tmp/rocksdb_blob_cloud";

// This is the name of the cloud storage bucket where the db
// is made durable. if you are using AWS, you have to manually
// ensure that this bucket name is unique to you and does not
// conflict with any other S3 users who might have already created
// this bucket name.
std::string kBucketSuffix = "cloud.durable.example.";
std::string kRegion = "us-west-2";

static const bool flushAtEnd = true;
static const bool disableWAL = false;

int main() {
  // cloud environment config options here
  rocksdb::titandb::TitanRouterFileSystem::Options router_options;

  router_options.credentials.InitializeSimple(
      getenv("AWS_ACCESS_KEY_ID"), getenv("AWS_SECRET_ACCESS_KEY"));
  if (!router_options.credentials.HasValid().ok()) {
    fprintf(
        stderr,
        "Please set env variables "
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return -1;
  }

  // Append the user name to the bucket name in an attempt to make it
  // globally unique. S3 bucket-names need to be globally unique.
  // If you want to rerun this example, then unique user-name suffix here.
  char *user = getenv("USER");
  kBucketSuffix.append(user);

  // "rockset." is the default bucket prefix
  const std::string bucketPrefix = "rockset.";
  router_options.src_bucket.SetBucketName(kBucketSuffix, bucketPrefix);
  router_options.dest_bucket.SetBucketName(kBucketSuffix, bucketPrefix);

  // create a bucket name for debugging purposes
  const std::string bucketName = bucketPrefix + kBucketSuffix;

  Aws::InitAPI(Aws::SDKOptions());
  // Create a new AWS cloud env Status
  rocksdb::CloudFileSystem *blob_cfs;
  rocksdb::Status s = rocksdb::CloudFileSystemEnv::NewAwsFileSystem(
      rocksdb::FileSystem::Default(), kBucketSuffix, kDBPath, kRegion,
      kBucketSuffix, kDBPath, kRegion, router_options, nullptr, &blob_cfs);
  if (!s.ok()) {
    fprintf(stderr, "Unable to create cloud env in bucket %s. %s\n",
            bucketName.c_str(), s.ToString().c_str());
    return -1;
  }
  std::shared_ptr<rocksdb::FileSystem> blob_cloud_fs(blob_cfs);
  router_options.blob_fs = blob_cloud_fs;
  auto base_fs = std::shared_ptr<rocksdb::FileSystem>(rocksdb::FileSystem::Default());

  // Create TitanRouterFileSystem
  std::unique_ptr<rocksdb::titandb::TitanRouterFileSystem> titan_router_file_system;
  s = rocksdb::titandb::TitanRouterFileSystem::NewTitanRouterFileSystem(base_fs, router_options, nullptr, &titan_router_file_system);
  assert(s.ok());

  // Create options and use the AWS file system that we created earlier
  auto cloud_env = NewCompositeEnv(blob_cloud_fs);
  rocksdb::titandb::TitanOptions options;
  options.env = cloud_env.get();

  // No persistent read-cache
  std::string persistent_cache = "";

  // options for each write
  rocksdb::WriteOptions wopt;
  wopt.disableWAL = disableWAL;  // TODO: figure out why set disable WAL

  // Open the DB
  rocksdb::titandb::TitanDB *db;
  options.min_blob_size = 10;
  options.create_if_missing = true;
  s = rocksdb::titandb::TitanDB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  s = db->Put(rocksdb::WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(rocksdb::ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    rocksdb::WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(rocksdb::WriteOptions(), &batch);
  }

  s = db->Get(rocksdb::ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(rocksdb::ReadOptions(), "key2", &value);
  assert(value == "value");

  db->Put(rocksdb::WriteOptions(), "key_large", "value_i_am_large");

  db->Flush(rocksdb::FlushOptions());

  delete db;
}
