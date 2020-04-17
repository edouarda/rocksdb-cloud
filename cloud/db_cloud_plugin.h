// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <vector>

#include "rocksdb/db_plugin.h"

namespace ROCKSDB_NAMESPACE {
namespace cloud {
struct CloudPluginOptions {
  CloudPluginOptions(const std::string& path = "",
                     uint64_t size = 0)
    : persistent_cache_path(path),
      persistent_cache_size_gb(size) {
  }
  // If you want sst files from S3 to be cached in local SSD/disk, then
  // persistent_cache_path should be the pathname of the local
  // cache storage.
  std::string persistent_cache_path;

  // The size of the persistent cache in GB
  uint64_t persistent_cache_size_gb = 0;
  // Maximum manifest file size
};

class CloudDBPlugin : public DBPlugin {
 public:
  static const uint64_t max_manifest_file_size = 4 * 1024L * 1024L;
  CloudDBPlugin();
  CloudDBPlugin(const std::string persistent_cache_path,
                uint64_t persistent_cache_size_gb);
  const char* Name() const override;
  Status SanitizeCB(
      OpenMode mode, const std::string& db_name, DBOptions* db_options,
      std::vector<ColumnFamilyDescriptor>* column_families) override;
  Status ValidateCB(
      OpenMode mode, const std::string& db_name, const DBOptions& db_options,
      const std::vector<ColumnFamilyDescriptor>& column_families) const override;
  Status OpenCB(OpenMode mode, DB* db, const std::vector<ColumnFamilyHandle*>& handles,
                DB** wrapped) override;
  bool SupportsOpenMode(OpenMode mode) const override {
    return (mode == OpenMode::Normal ||
            mode == OpenMode::ReadOnly);
  }
private:
  CloudPluginOptions plugin_options;
};

} // namespace cloud
} // namespace ROCKSDB_NAMESPACE
#endif // ROCKSDB_LITE
