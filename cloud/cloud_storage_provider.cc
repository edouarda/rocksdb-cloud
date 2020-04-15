//  Copyright (c) 2016-present, Rockset, Inc.  All rights reserved.
//

#include "rocksdb/cloud/cloud_storage_provider.h"

#include <cinttypes>
#include <mutex>
#include <set>

#include "cloud/cloud_env_impl.h"
#include "cloud/cloud_storage_provider_impl.h"
#include "cloud/filename.h"
#include "file/filename.h"
#include "options/customizable_helper.h"
#include "rocksdb/cloud/cloud_env_options.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/coding.h"
#include "util/stderr_logger.h"
#include "util/string_util.h"

namespace rocksdb {
/******************** Readablefile ******************/
CloudStorageReadableFileImpl::CloudStorageReadableFileImpl(
    const std::shared_ptr<Logger>& info_log, const std::string& bucket,
    const std::string& fname, uint64_t file_size)
    : info_log_(info_log),
      bucket_(bucket),
      fname_(fname),
      offset_(0),
      file_size_(file_size) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CloudReadableFile opening file %s", Name(), fname_.c_str());
}

// sequential access, read data at current offset in file
Status CloudStorageReadableFileImpl::Read(size_t n, Slice* result,
                                          char* scratch) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CloudReadableFile reading %s %ld", Name(), fname_.c_str(), n);
  Status s = Read(offset_, n, result, scratch);

  // If the read successfully returned some data, then update
  // offset_
  if (s.ok()) {
    offset_ += result->size();
  }
  return s;
}

// random access, read data from specified offset in file
Status CloudStorageReadableFileImpl::Read(uint64_t offset, size_t n,
                                          Slice* result, char* scratch) const {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CloudReadableFile reading %s at offset %" PRIu64
      " size %" ROCKSDB_PRIszt,
      Name(), fname_.c_str(), offset, n);

  *result = Slice();

  if (offset >= file_size_) {
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] CloudReadableFile reading %s at offset %" PRIu64
        " filesize %" PRIu64 ". Nothing to do",
        Name(), fname_.c_str(), offset, file_size_);
    return Status::OK();
  }

  // trim size if needed
  if (offset + n > file_size_) {
    n = file_size_ - offset;
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] CloudReadableFile reading %s at offset %" PRIu64
        " trimmed size %ld",
        Name(), fname_.c_str(), offset, n);
  }
  uint64_t bytes_read;
  Status st = DoCloudRead(offset, n, scratch, &bytes_read);
  if (st.ok()) {
    *result = Slice(scratch, bytes_read);
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[%s] CloudReadableFile file %s filesize %" PRIu64 " read %" PRIu64
        " bytes",
        Name(), fname_.c_str(), file_size_, bytes_read);
  }
  return st;
}

Status CloudStorageReadableFileImpl::Skip(uint64_t n) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[%s] CloudReadableFile file %s skip %" PRIu64, Name(), fname_.c_str(),
      n);
  // Update offset_ so that it does not go beyond filesize
  offset_ += n;
  if (offset_ > file_size_) {
    offset_ = file_size_;
  }
  return Status::OK();
}

size_t CloudStorageReadableFileImpl::GetUniqueId(char* id,
                                                 size_t max_size) const {
  // If this is an SST file name, then it can part of the persistent cache.
  // We need to generate a unique id for the cache.
  // If it is not a sst file, then nobody should be using this id.
  uint64_t file_number;
  FileType file_type;
  WalFileType log_type;
  ParseFileName(RemoveEpoch(basename(fname_)), &file_number, &file_type,
                &log_type);
  if (max_size < kMaxVarint64Length && file_number > 0) {
    char* rid = id;
    rid = EncodeVarint64(rid, file_number);
    return static_cast<size_t>(rid - id);
  }
  return 0;
}

/******************** Writablefile ******************/

CloudStorageWritableFileImpl::CloudStorageWritableFileImpl(
    CloudEnv* env, const std::string& local_fname, const std::string& bucket,
    const std::string& cloud_fname, const EnvOptions& options)
    : env_(env),
      fname_(local_fname),
      bucket_(bucket),
      cloud_fname_(cloud_fname) {
  auto fname_no_epoch = RemoveEpoch(fname_);
  // Is this a manifest file?
  is_manifest_ = IsManifestFile(fname_no_epoch);
  assert(IsSstFile(fname_no_epoch) || is_manifest_);

  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[%s] CloudWritableFile bucket %s opened local file %s "
      "cloud file %s manifest %d",
      Name(), bucket.c_str(), fname_.c_str(), cloud_fname.c_str(),
      is_manifest_);

  auto* file_to_open = &fname_;
  auto local_env = env_->GetBaseEnv();
  Status s;
  if (is_manifest_) {
    s = local_env->FileExists(fname_);
    if (!s.ok() && !s.IsNotFound()) {
      status_ = s;
      return;
    }
    if (s.ok()) {
      // Manifest exists. Instead of overwriting the MANIFEST (which could be
      // bad if we crash mid-write), write to the temporary file and do an
      // atomic rename on Sync() (Sync means we have a valid data in the
      // MANIFEST, so we can crash after it)
      tmp_file_ = fname_ + ".tmp";
      file_to_open = &tmp_file_;
    }
  }

  s = local_env->NewWritableFile(*file_to_open, &local_file_, options);
  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, env_->info_log_,
        "[%s] CloudWritableFile src %s %s", Name(), fname_.c_str(),
        s.ToString().c_str());
    status_ = s;
  }
}

CloudStorageWritableFileImpl::~CloudStorageWritableFileImpl() {
  if (local_file_ != nullptr) {
    Close();
  }
}

Status CloudStorageWritableFileImpl::Close() {
  if (local_file_ == nullptr) {  // already closed
    return status_;
  }
  Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
      "[%s] CloudWritableFile closing %s", Name(), fname_.c_str());
  assert(status_.ok());

  // close local file
  Status st = local_file_->Close();
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, env_->info_log_,
        "[%s] CloudWritableFile closing error on local %s\n", Name(),
        fname_.c_str());
    return st;
  }
  local_file_.reset();

  if (!is_manifest_) {
    status_ = env_->CopyLocalFileToDest(fname_, cloud_fname_);
    if (!status_.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, env_->info_log_,
          "[%s] CloudWritableFile closing PutObject failed on local file %s",
          Name(), fname_.c_str());
      return status_;
    }

    // delete local file
    if (!env_->GetCloudEnvOptions().keep_local_sst_files) {
      status_ = env_->GetBaseEnv()->DeleteFile(fname_);
      if (!status_.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, env_->info_log_,
            "[%s] CloudWritableFile closing delete failed on local file %s",
            Name(), fname_.c_str());
        return status_;
      }
    }
    Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
        "[%s] CloudWritableFile closed file %s", Name(), fname_.c_str());
  }
  return Status::OK();
}

// Sync a file to stable storage
Status CloudStorageWritableFileImpl::Sync() {
  if (local_file_ == nullptr) {
    return status_;
  }
  assert(status_.ok());

  // sync local file
  Status stat = local_file_->Sync();

  if (stat.ok() && !tmp_file_.empty()) {
    assert(is_manifest_);
    // We are writing to the temporary file. On a first sync we need to rename
    // the file to the real filename.
    stat = env_->GetBaseEnv()->RenameFile(tmp_file_, fname_);
    // Note: this is not thread safe, but we know that manifest writes happen
    // from the same thread, so we are fine.
    tmp_file_.clear();
  }

  // We copy MANIFEST to cloud on every Sync()
  if (is_manifest_ && stat.ok()) {
    stat = env_->CopyLocalFileToDest(fname_, cloud_fname_);
    if (stat.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, env_->info_log_,
          "[%s] CloudWritableFile made manifest %s durable to "
          "bucket %s bucketpath %s.",
          Name(), fname_.c_str(), bucket_.c_str(), cloud_fname_.c_str());
    } else {
      Log(InfoLogLevel::ERROR_LEVEL, env_->info_log_,
          "[%s] CloudWritableFile failed to make manifest %s durable to "
          "bucket %s bucketpath %s: %s",
          Name(), fname_.c_str(), bucket_.c_str(), cloud_fname_.c_str(),
          stat.ToString().c_str());
    }
  }
  return stat;
}

Status CloudStorageProvider::CreateFromString(const std::string& value,
                                              const ConfigOptions& opts,
                                              std::shared_ptr<CloudStorageProvider>* result) {
  return LoadSharedObject<CloudStorageProvider>(value, nullptr, opts, result);
}

CloudStorageProvider::~CloudStorageProvider() {}

CloudStorageProviderImpl::CloudStorageProviderImpl() : rng_(time(nullptr)) {}

CloudStorageProviderImpl::~CloudStorageProviderImpl() {}

Status CloudStorageProviderImpl::PrepareOptions(CloudEnv *cloud_env, const ConfigOptions& opts) {
  Status s;
  auto & cloud_opts = cloud_env->GetCloudEnvOptions();
  auto & provider = cloud_opts.storage_provider;
  if (provider) {
    ConfigOptions copy = opts;
    copy.env = cloud_env;
    s = provider->PrepareOptions(copy);
    if (!s.ok()) {
      return s;
    } else if (cloud_env->HasDestBucket()) {
      // create dest bucket if specified
      if (provider->ExistsBucket(cloud_env->GetDestBucketName()).ok()) {
        Log(InfoLogLevel::INFO_LEVEL, cloud_env->info_log_,
            "[%s] Bucket %s already exists", provider->Name(),
            cloud_env->GetDestBucketName().c_str());
      } else if (cloud_opts.create_bucket_if_missing) {
        Log(InfoLogLevel::INFO_LEVEL, cloud_env->info_log_,
            "[%s] Going to create bucket %s", provider->Name(),
            cloud_env->GetDestBucketName().c_str());
        s = provider->CreateBucket(cloud_env->GetDestBucketName());
      } else {
        s = Status::NotFound("Bucket not found and create_bucket_if_missing is false");
      }
      if (!s.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, cloud_env->info_log_,
            "[%s] Unable to create bucket %s %s", provider->Name(),
            cloud_env->GetDestBucketName().c_str(), s.ToString().c_str());
      }
    }
  } else if (cloud_opts.dest_bucket.IsValid() ||
             cloud_opts.src_bucket.IsValid()) {
    s = Status::InvalidArgument("Cloud environment requires a storage provider");
  }
  return s;
}

Status CloudStorageProviderImpl::PrepareOptions(const ConfigOptions& opts) {
  auto *cloud_env = opts.env->CastAs<CloudEnv>(CloudOptionNames::kNameCloud);
  if (cloud_env != nullptr) {
    env_ = cloud_env;
    status_ = CloudStorageProvider::PrepareOptions(opts);
  } else {
    status_ = Status::InvalidArgument("StorageProvider requires cloud env ", opts.env->GetId());
  }
  return status_;
}

Status CloudStorageProviderImpl::ValidateOptions(const DBOptions&db_opts,
                                                 const ColumnFamilyOptions& cf_opts) const {
  if (!status_.ok()) {
    return status_;
  } else if (env_ == nullptr) {
    return Status::InvalidArgument("StorageProvider requires cloud env ", GetId());
  } 
  return CloudStorageProvider::ValidateOptions(db_opts, cf_opts);
}
  
Status CloudStorageProviderImpl::NewCloudReadableFile(
    const std::string& bucket, const std::string& fname,
    std::unique_ptr<CloudStorageReadableFile>* result,
    const EnvOptions& options) {
  // First, check if the file exists and also find its size. We use size in
  // CloudReadableFile to make sure we always read the valid ranges of the file
  uint64_t size;
  Status st = GetObjectSize(bucket, fname, &size);
  if (!st.ok()) {
    return st;
  }
  return DoNewCloudReadableFile(bucket, fname, size, result, options);
}

Status CloudStorageProviderImpl::GetObject(
    const std::string& bucket_name, const std::string& object_path,
    const std::string& local_destination) {
  Env* localenv = env_->GetBaseEnv();
  std::string tmp_destination =
      local_destination + ".tmp-" + std::to_string(rng_.Next());

  uint64_t remote_size;
  Status s =
      DoGetObject(bucket_name, object_path, tmp_destination, &remote_size);
  if (!s.ok()) {
    localenv->DeleteFile(tmp_destination);
    return s;
  }

  // Check if our local file is the same as promised
  uint64_t local_size{0};
  s = localenv->GetFileSize(tmp_destination, &local_size);
  if (!s.ok()) {
    return s;
  }
  if (local_size != remote_size) {
    localenv->DeleteFile(tmp_destination);
    s = Status::IOError("Partial download of a file " + local_destination);
    Log(InfoLogLevel::ERROR_LEVEL, env_->info_log_,
        "[%s] GetObject %s/%s local size %" PRIu64
        " != cloud size "
        "%" PRIu64 ". %s",
        Name(), bucket_name.c_str(), object_path.c_str(), local_size,
        remote_size, s.ToString().c_str());
  }

  if (s.ok()) {
    s = localenv->RenameFile(tmp_destination, local_destination);
  }
  Log(InfoLogLevel::INFO_LEVEL, env_->info_log_,
      "[%s] GetObject %s/%s size %" PRIu64 ". %s", bucket_name.c_str(), Name(),
      object_path.c_str(), local_size, s.ToString().c_str());
  return s;
}

Status CloudStorageProviderImpl::PutObject(const std::string& local_file,
                                           const std::string& bucket_name,
                                           const std::string& object_path) {
  uint64_t fsize = 0;
  // debugging paranoia. Files uploaded to Cloud can never be zero size.
  auto st = env_->GetBaseEnv()->GetFileSize(local_file, &fsize);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, env_->info_log_,
        "[%s] PutObject localpath %s error getting size %s", Name(),
        local_file.c_str(), st.ToString().c_str());
    return st;
  }
  if (fsize == 0) {
    Log(InfoLogLevel::ERROR_LEVEL, env_->info_log_,
        "[%s] PutObject localpath %s error zero size", Name(),
        local_file.c_str());
    return Status::IOError(local_file + " Zero size.");
  }

  return DoPutObject(local_file, bucket_name, object_path, fsize);
}

}  // namespace rocksdb
