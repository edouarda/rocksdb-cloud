// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE


#include "cloud/aws/aws_env.h"
#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "port/likely.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"

namespace rocksdb {
static CloudEnvOptions dummy_cloud_options;

template <typename T1>
int offset_of(T1 CloudEnvOptions::*member) {
  return int(size_t(&(dummy_cloud_options.*member)) - size_t(&dummy_cloud_options));
}

static std::unordered_map<std::string, OptionTypeInfo> basic_cloud_type_info = {
    {"keep_local_log_files", {offset_of(&CloudEnvOptions::keep_local_log_files), OptionType::kBoolean}},
    {"keep_local_sst_files", {offset_of(&CloudEnvOptions::keep_local_sst_files), OptionType::kBoolean}},
    {"purger_periodicity_millis",
     {offset_of(&CloudEnvOptions::purger_periodicity_millis), OptionType::kUInt64T}},
    {"validate_file_size",
     {offset_of(&CloudEnvOptions::validate_filesize), OptionType::kBoolean}},
    {"create_bucket_if_missing",
     {offset_of(&CloudEnvOptions::create_bucket_if_missing), OptionType::kBoolean}},
    {"request_timeout_ms",
     {offset_of(&CloudEnvOptions::request_timeout_ms), OptionType::kUInt64T}},
    {"run_purger",
     {offset_of(&CloudEnvOptions::run_purger), OptionType::kBoolean}},
    {"ephemeral_resync_on_open",
     {offset_of(&CloudEnvOptions::ephemeral_resync_on_open), OptionType::kBoolean}},
    {"skip_dbid_verification",
     {offset_of(&CloudEnvOptions::skip_dbid_verification), OptionType::kBoolean}},
    {"bucket.source",
     {offset_of(&CloudEnvOptions::src_bucket), OptionType::kStruct,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      [](const std::string& name, const std::string& value,
         const ConfigOptions& opts, char* addr) {
        Status s = OptionTypeInfo::ParseStruct(
                        "bucket.source", BucketOptions::GetBucketTypeInfo(),
                        name, value, opts, addr);
        if (s.ok()) {
          auto *bucket = reinterpret_cast<BucketOptions*>(addr);
          bucket->Initialize();
        }
        return s;
      },
      [](const std::string& name, const char* addr,
         const ConfigOptions& opts, std::string* value) {
        return OptionTypeInfo::SerializeStruct(
                        "bucket.source", BucketOptions::GetBucketTypeInfo(),
                        name, addr, opts, value);
      },
      [](const std::string& name, const char* addr1, const char* addr2,
         const ConfigOptions& opts, std::string* mismatch) {
        const auto *bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
        const auto *bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
        if (*bucket1 == *bucket2) {
          return true;
        } else {
          return OptionTypeInfo::MatchesStruct(
                        "bucket.source", BucketOptions::GetBucketTypeInfo(),
                        name, addr1, addr2, opts, mismatch);
        }
      }
     }},
    {"bucket.dest",
     {offset_of(&CloudEnvOptions::dest_bucket), OptionType::kStruct,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone,
      [](const std::string& name, const std::string& value,
         const ConfigOptions& opts, char* addr) {
        Status s = OptionTypeInfo::ParseStruct(
                        "bucket.dest", BucketOptions::GetBucketTypeInfo(),
                        name, value, opts, addr);
        if (s.ok()) {
          auto *bucket = reinterpret_cast<BucketOptions*>(addr);
          bucket->Initialize();
        }
        return s;
      },
      [](const std::string& name, const char* addr,
         const ConfigOptions& opts, std::string* value) {
        return OptionTypeInfo::SerializeStruct(
                        "bucket.dest", BucketOptions::GetBucketTypeInfo(),
                        name, addr, opts, value);
      },
      [](const std::string& name, const char* addr1, const char* addr2,
         const ConfigOptions& opts, std::string* mismatch) {
        const auto *bucket1 = reinterpret_cast<const BucketOptions*>(addr1);
        const auto *bucket2 = reinterpret_cast<const BucketOptions*>(addr2);
        if (*bucket1 == *bucket2) {
          return true;
        } else {
          return OptionTypeInfo::MatchesStruct(
                        "bucket.dest", BucketOptions::GetBucketTypeInfo(),
                        name, addr1, addr2, opts, mismatch);
        }
      }
     }},
    {"storage_provider",
      OptionTypeInfo::AsCustomS<CloudStorageProvider>(
          offset_of(&CloudEnvOptions::storage_provider),
          OptionVerificationType::kByName,
          OptionTypeFlags::kAllowNull | OptionTypeFlags::kDontPrepare)
    },
    {"log_controller",
      OptionTypeInfo::AsCustomS<CloudLogController>(
          offset_of(&CloudEnvOptions::cloud_log_controller),
          OptionVerificationType::kByNameAllowFromNull,
          OptionTypeFlags::kAllowNull | OptionTypeFlags::kDontPrepare)
    },
};

//**TODO: If a CloudEnv extends EnvWrapper. this can go away
static std::unordered_map<std::string, OptionTypeInfo> env_target_type_info = {
    {"target",
     OptionTypeInfo::AsCustomP<Env>(0, OptionVerificationType::kByName, OptionTypeFlags::kNone)
    },
};

CloudEnv::CloudEnv(const CloudEnvOptions& options, Env *base, const std::shared_ptr<Logger>& logger)
  : cloud_env_options(options),
    base_env_(base),
    info_log_(logger) {
  RegisterOptions("BaseEnvOptions", &base_env_, &env_target_type_info); //**See TODO above
  RegisterOptions(kCloudEnvOpts, &cloud_env_options, &basic_cloud_type_info);
  
}
  
CloudEnv::~CloudEnv() {}
  
const Customizable* CloudEnv::FindInstance(const std::string& name) const {
  if (name == kCloudEnvName) {
    return this;
  } else {
    return Env::FindInstance(name);
  }
}

Status CloudEnv::NewAwsEnv(
    Env* base_env, const std::string& src_cloud_bucket,
    const std::string& src_cloud_object, const std::string& src_cloud_region,
    const std::string& dest_cloud_bucket, const std::string& dest_cloud_object,
    const std::string& dest_cloud_region, const CloudEnvOptions& cloud_options,
    const std::shared_ptr<Logger>& logger, CloudEnv** cenv) {
  CloudEnvOptions options = cloud_options;
  if (!src_cloud_bucket.empty()) options.src_bucket.SetBucketName(src_cloud_bucket);
  if (!src_cloud_object.empty()) options.src_bucket.SetObjectPath(src_cloud_object);
  if (!src_cloud_region.empty()) options.src_bucket.SetRegion(src_cloud_region);
  if (!dest_cloud_bucket.empty()) options.dest_bucket.SetBucketName(dest_cloud_bucket);
  if (!dest_cloud_object.empty()) options.dest_bucket.SetObjectPath(dest_cloud_object);
  if (!dest_cloud_region.empty()) options.dest_bucket.SetRegion(dest_cloud_region);
  return NewAwsEnv(base_env, options, logger, cenv);
}

#ifndef USE_AWS
Status CloudEnv::NewAwsEnv(Env* /*base_env*/,
                           const CloudEnvOptions& /*options*/,
                           const std::shared_ptr<Logger>& /*logger*/,
                           CloudEnv** /*cenv*/) {
  return Status::NotSupported("RocksDB Cloud not compiled with AWS support");
}
#else
Status CloudEnv::NewAwsEnv(Env* base_env,
                           const CloudEnvOptions& options,
                           const std::shared_ptr<Logger> & logger, CloudEnv** cenv) {
  // Dump out cloud env options
  options.Dump(logger.get());

  Status st = AwsEnv::NewAwsEnv(base_env, options, logger, cenv);
  if (st.ok()) {
    // store a copy of the logger
    CloudEnvImpl* cloud = static_cast<CloudEnvImpl*>(*cenv);
    cloud->info_log_ = logger;

    // start the purge thread only if there is a destination bucket
    if (options.dest_bucket.IsValid() && options.run_purger) {
      cloud->purge_thread_ = std::thread([cloud] { cloud->Purger(); });
    }
  }
  return st;
}
#endif

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
