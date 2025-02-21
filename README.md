# Titan: A RocksDB Plugin to Reduce Write Amplification

[![Build Status](https://travis-ci.org/tikv/titan.svg?branch=master)](https://travis-ci.org/tikv/titan)
[![codecov](https://codecov.io/gh/tikv/titan/branch/master/graph/badge.svg)](https://codecov.io/gh/tikv/titan)

Titan is a RocksDB Plugin for key-value separation, inspired by 
[WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).
For introduction and design details, see our
[blog post](https://pingcap.com/blog/titan-storage-engine-design-and-implementation/).

## Build and Test
Titan relies on RocksDB source code to build. You need to checkout RocksDB source code locally,
and provide the path to Titan build script.
```
# To build:
mkdir -p build
cd build
cmake ..
make -j<n>

# To specify custom rocksdb
cmake .. -DROCKSDB_DIR=<rocksdb_source_dir>
# or
cmake .. -DROCKSDB_GIT_REPO=<git_repo> -DROCKSDB_GIT_BRANCH=<branch>

# Build static lib (i.e. libtitan.a) only:
make titan -j<n>

# Release build:
cmake .. -DROCKSDB_DIR=<rocksdb_source_dir> -DCMAKE_BUILD_TYPE=Release

# Building with sanitizer (e.g. ASAN):
cmake .. -DROCKSDB_DIR=<rocksdb_source_dir> -DWITH_ASAN=ON

# Building with compression libraries (e.g. snappy):
cmake .. -DROCKSDB_DIR=<rocksdb_source_dir> -DWITH_SNAPPY=ON

# Run tests after build. You need to filter tests by "titan" prefix.
ctest -R titan

# To format code, install clang-format and run the script.
bash scripts/format-diff.sh
```

### Titan on S3
Follow the steps listed in https://github.com/aws/aws-sdk-cpp  to install the c++ AWS sdk.
```
# Install dependency
sudo apt install libcurl4-openssl-dev

git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp

cd aws-sdk-cpp; mkdir build; cd build

# For example
cmake .. \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_INSTALL_PREFIX=<path-to-install> \
-DBUILD_ONLY="s3;kinesis;transfer"

make -j 
sudo make install
```
Follow the steps listed in https://github.com/rockset/rocksdb-cloud/tree/master/cloud to set env variables.

```
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export AWS_SESSION_TOKEN=
```
Build Titan on S3
```
git clone https://github.com/homeffjy/titan.git

cd titan

git checkout dev-fjy/port-rocksdb-cloud

mkdir build; cd build

# For example
cmake .. \
-DCMAKE_BUILD_TYPE=Release \
-DROCKSDB_GIT_REPO=https://github.com/homeffjy/rocksdb \
-DROCKSDB_GIT_BRANCH=dev-fjy/port-rocksdb-cloud \
-DWITH_AWS=ON \
-DUSE_RTTI=ON

make -j
```
Run benchmark with `--use-cloud` to enable s3, `--s3_bucket` to set s3 bucket name, `--s3_region` to set s3 region

You may need to ensure that AWS EC2 and AWS S3 are in the same region to guarantee network bandwidth.
```
# For example
./titandb_bench --use_titan --use_cloud --compression_type=none
```
## Compatibility with RocksDB

Current version of Titan is developed and tested with TiKV's [fork][6.29.tikv] of RocksDB 6.29.
Another version that is based off TiKV's [fork][6.4.tikv] of RocksDB 6.4 can be found in the [tikv-6.1 branch][tikv-6.1].

[6.4.tikv]: https://github.com/tikv/rocksdb/tree/6.4.tikv
[6.29.tikv]: https://github.com/tikv/rocksdb/tree/6.29.tikv
[tikv-6.1]: https://github.com/tikv/titan/tree/tikv-6.1
