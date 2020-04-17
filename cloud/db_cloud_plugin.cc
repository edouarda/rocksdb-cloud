// Copyright (c) 2017 Rockset.
#include "cloud/db_cloud_plugin.h"

#include <cinttypes>

#include "cloud/cloud_env_impl.h"
#include "cloud/db_cloud_impl.h"
#include "env/composite_env_wrapper.h"
#include "file/sst_file_manager_impl.h"
#include "logging/auto_roll_logger.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/db_plugin.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/table.h"

#ifndef ROCKSDB_LITE
namespace ROCKSDB_NAMESPACE {
namespace cloud {
namespace {
/**
 * This ConstantSstFileManager uses the same size for every sst files added.
 */
class ConstantSizeSstFileManager : public SstFileManagerImpl {
 public:
  ConstantSizeSstFileManager(int64_t constant_file_size, Env* env,
                             std::shared_ptr<Logger> logger,
                             int64_t rate_bytes_per_sec,
                             double max_trash_db_ratio,
                             uint64_t bytes_max_delete_chunk)
    : SstFileManagerImpl(env, std::make_shared<LegacyFileSystemWrapper>(env),
                         std::move(logger), rate_bytes_per_sec,
                         max_trash_db_ratio, bytes_max_delete_chunk),
        constant_file_size_(constant_file_size) {
    assert(constant_file_size_ >= 0);
  }

  Status OnAddFile(const std::string& file_path, bool compaction) override {
    return SstFileManagerImpl::OnAddFile(
        file_path, uint64_t(constant_file_size_), compaction);
  }

 private:
  const int64_t constant_file_size_;
};
}  // namespace

  
CloudDBPlugin::CloudDBPlugin() {
}

CloudDBPlugin::CloudDBPlugin(const std::string persistent_cache_path,
                             uint64_t persistent_cache_size)
  : plugin_options(persistent_cache_path, persistent_cache_size) {
}
  
const char* CloudDBPlugin::Name() const {
  return "Cloud";
}

Status CloudDBPlugin::SanitizeCB(
      OpenMode mode, const std::string& db_name, DBOptions* db_options,
      std::vector<ColumnFamilyDescriptor>* column_families) {
  auto *cenv = db_options->env->CastAs<CloudEnv>(CloudOptionNames::kNameCloud);
  if (cenv == nullptr) {
    return Status::InvalidArgument("CloudDB requires CloudEnv");
  }
  
  // Created logger if it is not already pre-created by user.
  if (db_options->info_log) {
    CreateLoggerFromOptions(db_name, *db_options, &(db_options->info_log));
  }
  if (!cenv->info_log_) {
    cenv->info_log_ = db_options->info_log;
  }

  auto & cloud_opts = cenv->GetCloudEnvOptions();
  // Use a constant sized SST File Manager if necesary.
  // NOTE: if user already passes in an SST File Manager, we will respect user's
  // SST File Manager instead.
  if (db_options->sst_file_manager == nullptr) {
    auto constant_sst_file_size =
      cloud_opts.constant_sst_file_size_in_sst_file_manager;
    if (constant_sst_file_size >= 0) {
      // rate_bytes_per_sec, max_trash_db_ratio, bytes_max_delete_chunk are
      // default values in NewSstFileManager.
      // If users don't use Options.sst_file_manager, then these values are used
      // currently when creating an SST File Manager.
      db_options->sst_file_manager =
        std::make_shared<ConstantSizeSstFileManager>(
                constant_sst_file_size, cenv, db_options->info_log,
                0 /* rate_bytes_per_sec */, 0.25 /* max_trash_db_ratio */,
                64 * 1024 * 1024 /* bytes_max_delete_chunk */);
    }
  }
  bool read_only = mode == OpenMode::ReadOnly;
  Env* local_env = cenv->GetBaseEnv();
  if (!read_only) {
    local_env->CreateDirIfMissing(db_name); 
  }
  
  Status st;
  auto *cimpl = cenv->CastAs<CloudEnvImpl>(CloudEnvImpl::kCloudEnvImplName);
  if (cimpl != nullptr) {
    st = cimpl->SanitizeDirectory(*db_options, db_name, read_only);
    if (st.ok()) {
      st = cimpl->LoadCloudManifest(db_name, read_only);
    }
    if (!st.ok()) {
      return st;
    }
  }
  // We do not want a very large MANIFEST file because the MANIFEST file is
  // uploaded to S3 for every update, so always enable rolling of Manifest file
  if (db_options->max_manifest_file_size > CloudDBPlugin::max_manifest_file_size) {
    db_options->max_manifest_file_size = CloudDBPlugin::max_manifest_file_size;
  }


  // If a persistent cache path is specified, then we set it in the table options.
  if (plugin_options.persistent_cache_size_gb > 0 &&
      !plugin_options.persistent_cache_path.empty()) {
    std::shared_ptr<PersistentCache> pcache;
    for (auto & cfd : *column_families) {
      // Get existing options. If the persistent cache is already set, then do
      // not make any change. Otherwise, configure it.
      auto bopt = cfd.options.table_factory->GetOptions
        <BlockBasedTableOptions>(TableFactory::kBlockBasedTableOpts);
      if (bopt != nullptr && !bopt->persistent_cache) {
        if (pcache == nullptr) {
          st = NewPersistentCache(
                db_options->env, plugin_options.persistent_cache_path,
                plugin_options.persistent_cache_size_gb * 1024L * 1024L * 1024L,
                db_options->info_log, false, &pcache);
          if (!st.ok()) {
            Log(InfoLogLevel::INFO_LEVEL, db_options->info_log,
                "Unable to create persistent cache %s. %s",
                plugin_options.persistent_cache_path.c_str(), st.ToString().c_str());
            return st;
          } else {
            Log(InfoLogLevel::INFO_LEVEL, db_options->info_log,
                "Created persistent cache %s with size %" PRIu64 "GB",
                plugin_options.persistent_cache_path.c_str(),
                plugin_options.persistent_cache_size_gb);
          }
        }
        bopt->persistent_cache = pcache;
      }
    }
  }
  return Status::OK();
}
  
Status CloudDBPlugin::ValidateCB(
           OpenMode mode, const std::string& db_name, const DBOptions& db_options,
           const std::vector<ColumnFamilyDescriptor>& column_families) const {
  auto *cenv = db_options.env->CastAs<CloudEnv>(CloudOptionNames::kNameCloud);
  if (cenv == nullptr) {
    return Status::InvalidArgument("CloudDB requires CloudEnv");
  }
  return DBPlugin::ValidateCB(mode, db_name, db_options, column_families);
}
  
Status CloudDBPlugin::OpenCB(OpenMode /*mode*/, DB* db,
                             const std::vector<ColumnFamilyHandle*>& /*handles*/,
                             DB** wrapped) {
  auto *cenv = db->GetEnv()->CastAs<CloudEnv>(CloudOptionNames::kNameCloud);
  printf("MJR: OpenCloudDB cenv=%p\n", cenv);
  assert(cenv);
  if (cenv == nullptr) {
    return Status::InvalidArgument("CloudDB requires CloudEnv");
  } else {
    DBCloud *cloud = new DBCloudImpl(db);
    *wrapped = cloud;
    // now that the database is opened, all file sizes have been verified and we
    // no longer need to verify file sizes for each file that we open. Note that
    // this might have a data race with background compaction, but it's not a big
    // deal, since it's a boolean and it does not impact correctness in any way.
    if (cenv->GetCloudEnvOptions().validate_filesize) {
      *const_cast<bool*>(&cenv->GetCloudEnvOptions().validate_filesize) = false;
    }
    return Status::OK();
  }
}


} // namespace cloud
} // namespace ROCKSDB_NAMESPACE
#endif // ROCKSDB_LITE
