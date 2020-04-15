// Copyright (c) 2017 Rockset.
#include "rocksdb/cloud/cloud_env_options.h"
#ifndef ROCKSDB_LITE

#include <cinttypes>
#include <unistd.h>

#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_env_wrapper.h"
#include "cloud/db_cloud_impl.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/options_type.h"

namespace rocksdb {
BucketOptions::BucketOptions() {
  prefix_ = "rockset.";
}

const std::unordered_map<std::string, OptionTypeInfo> *BucketOptions::GetBucketTypeInfo() {
  static std::unordered_map<std::string, OptionTypeInfo> bucket_type_info = {
   {"region",  {offsetof(class BucketOptions, region_), OptionType::kString}},
   {"object",  {offsetof(class BucketOptions, object_), OptionType::kString}},
   {"prefix",  {offsetof(class BucketOptions, prefix_), OptionType::kString}},
   {"bucket",  {offsetof(class BucketOptions, bucket_), OptionType::kString}},
  };
  return &bucket_type_info;
}

Status BucketOptions::ConfigureFromString(const std::string& value,
                                          const ConfigOptions& options) {
  Status s = OptionTypeInfo::ParseStruct("", GetBucketTypeInfo(), "", value,
                                         options, reinterpret_cast<char*>(this));
  if (s.ok()) {
    Initialize();
  }
  return s;
}
  
Status BucketOptions::GetOptionString(const ConfigOptions& options,
                                      std::string* result) const {
  return OptionTypeInfo::SerializeStruct("bucket", GetBucketTypeInfo(), "bucket",
                                         reinterpret_cast<const char*>(this),
                                         options, result);
}

std::string BucketOptions::ToString(const ConfigOptions& options) const {
  std::string result;
  Status s = GetOptionString(options, &result);
  assert(s.ok());
  return result;
}

void BucketOptions::SetBucketName(const std::string& bucket,
                                  const std::string& prefix) {
  if (!prefix.empty()) {
    prefix_ = prefix;
  }
  bucket_ = bucket;
  Initialize();
}
  
void BucketOptions::Initialize() {
  if (bucket_.empty()) {
    name_.clear();
  } else {
    name_ = prefix_ + bucket_;
  }
}
// Initializes the bucket properties

void BucketOptions::TEST_Initialize(const std::string& bucket,
                                    const std::string& object,
                                    const std::string& region) {
  std::string prefix;
  // If the bucket name is not set, then the bucket name is not set,
  // Set it to either the value of the environment variable or geteuid
  if (!CloudEnvOptions::GetNameFromEnvironment("ROCKSDB_CLOUD_TEST_BUCKET_NAME",
                                               "ROCKSDB_CLOUD_BUCKET_NAME",
                                               &bucket_)) {
    bucket_ = bucket + std::to_string(geteuid());
  }
  if (CloudEnvOptions::GetNameFromEnvironment(
          "ROCKSDB_CLOUD_TEST_BUCKET_PREFIX", "ROCKSDB_CLOUD_BUCKET_PREFIX",
          &prefix)) {
    prefix_ = prefix;
  }
  name_ = prefix_ + bucket_;
  if (!CloudEnvOptions::GetNameFromEnvironment("ROCKSDB_CLOUD_TEST_OBECT_PATH",
                                               "ROCKSDB_CLOUD_OBJECT_PATH",
                                               &object_)) {
    object_ = object;
  }
  if (!CloudEnvOptions::GetNameFromEnvironment(
          "ROCKSDB_CLOUD_TEST_REGION", "ROCKSDB_CLOUD_REGION", &region_)) {
    region_ = region;
  }
}

bool CloudEnvOptions::GetNameFromEnvironment(const char *name, const char *alt, std::string * result) {

  char *value = getenv(name);               // See if name is set in the environment
  if (value == nullptr && alt != nullptr) { // Not set.  Do we have an alt name?
    value = getenv(alt);                    // See if alt is in the environment
  }
  if (value != nullptr) {                   // Did we find the either name/alt in the env?
    result->assign(value);                  // Yes, update result
    return true;                            // And return success
  } else {
    return false;                           // No, return not found
  }
}
void CloudEnvOptions::TEST_Initialize(const std::string& bucket,
                                      const std::string& object,
                                      const std::string& region) {
  src_bucket.TEST_Initialize(bucket, object, region);
  dest_bucket = src_bucket;
  credentials.TEST_Initialize();
}

void CloudEnvOptions::Dump(Logger* log) const {
  Header(log, "                         COptions.cloud_type: %u", cloud_type);
  Header(log, "                           COptions.log_type: %u", log_type);
  Header(log, "               COptions.keep_local_sst_files: %d",
         keep_local_sst_files);
  Header(log, "               COptions.keep_local_log_files: %d",
         keep_local_log_files);
  Header(log, "             COptions.server_side_encryption: %d",
         server_side_encryption);
  Header(log, "                  COptions.encryption_key_id: %s",
         encryption_key_id.c_str());
  Header(log, "           COptions.create_bucket_if_missing: %s",
         create_bucket_if_missing ? "true" : "false");
  Header(log, "                         COptions.run_purger: %s",
         run_purger ? "true" : "false");
  Header(log, "           COptions.ephemeral_resync_on_open: %s",
         ephemeral_resync_on_open ? "true" : "false");
  Header(log, "             COptions.skip_dbid_verification: %s",
         skip_dbid_verification ? "true" : "false");
  Header(log, "           COptions.use_aws_transfer_manager: %s",
         use_aws_transfer_manager ? "true" : "false");
  Header(log, "           COptions.number_objects_listed_in_one_iteration: %d",
         number_objects_listed_in_one_iteration);
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
