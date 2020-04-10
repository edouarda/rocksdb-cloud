//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/env.h"

#include <thread>

#include "env/composite_env_wrapper.h"
#include "logging/env_logger.h"
#include "memory/arena.h"
#include "options/customizable_helper.h"
#include "options/db_options.h"
#include "port/port.h"
#include "port/sys_time.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {
const std::string Env::kPosixEnvName = "Posix";
const std::string Env::kDefaultEnvName = "Default";
const std::string Env::kMemoryEnvName = "Memory";
const std::string Env::kTimedEnvName = "Timed";
const std::string Env::kEncryptedEnvName = "Encrypted";

Env::Env() : thread_status_updater_(nullptr) {
  file_system_ = std::make_shared<LegacyFileSystemWrapper>(this);
}

Env::Env(std::shared_ptr<FileSystem> fs)
  : thread_status_updater_(nullptr),
    file_system_(fs) {}

Env::~Env() {
}

Status Env::NewLogger(const std::string& fname,
                      std::shared_ptr<Logger>* result) {
  return NewEnvLogger(fname, this, result);
}

static bool LoadStaticEnv(const std::string& id, Env** result) {
  bool success = true;
  if (id.empty() || id == Env::kDefaultEnvName) {
    *result = Env::Default();
  } else if (id == Env::kPosixEnvName) {
    *result = Env::Default();
  } else {
    success = false;
  }
  return success;
}

static bool LoadSharedEnv(const std::string& id, std::shared_ptr<Env>* result) {
  bool success = true;
  assert(result);
  if (id.empty()) {
    success = false;
#ifndef ROCKSDB_LITE
  } else if (id == Env::kMemoryEnvName) {
    result->reset(NewMemEnv(nullptr));
  } else if (id == Env::kTimedEnvName) {
    result->reset(NewTimedEnv(nullptr));
#endif  // ROCKSDB_LITE
  } else {
    success = false;
  }
  return success;
}

Status Env::CreateFromString(const std::string& value,
                             const ConfigOptions& options, Env** result) {
  Env* env = *result;
  Status s = LoadStaticObject<Env>(value, LoadStaticEnv, options, &env);
  if (s.ok()) {
    *result = env;
  }
  return s;
}

Status Env::LoadEnv(const std::string& value, Env** result) {
  return CreateFromString(value, ConfigOptions(), result);
}

Status Env::CreateFromString(const std::string& value,
                             const ConfigOptions& options, Env** result,
                             std::shared_ptr<Env>* guard) {
  assert(result);
  assert(guard);
  if (!value.empty()) {
    // Since if we fail to load the object shared, we will try statically,
    // turn off unknown objects
    ConfigOptions copy = options;
    copy.ignore_unknown_objects = false;
    Status s = LoadSharedObject<Env>(value, LoadSharedEnv, copy, guard);
    if (s.ok()) {
      *result = guard->get();
      return s;
    } else if (!s.IsNotSupported()) {
      return s;
    }
  }
  return CreateFromString(value, options, result);
}

Status Env::LoadEnv(const std::string& value, Env** result,
                    std::shared_ptr<Env>* guard) {
  return CreateFromString(value, ConfigOptions(), result, guard);
}

std::string Env::PriorityToString(Env::Priority priority) {
  switch (priority) {
    case Env::Priority::BOTTOM:
      return "Bottom";
    case Env::Priority::LOW:
      return "Low";
    case Env::Priority::HIGH:
      return "High";
    case Env::Priority::USER:
      return "User";
    case Env::Priority::TOTAL:
      assert(false);
  }
  return "Invalid";
}

uint64_t Env::GetThreadID() const {
  std::hash<std::thread::id> hasher;
  return hasher(std::this_thread::get_id());
}

Status Env::ReuseWritableFile(const std::string& fname,
                              const std::string& old_fname,
                              std::unique_ptr<WritableFile>* result,
                              const EnvOptions& options) {
  Status s = RenameFile(old_fname, fname);
  if (!s.ok()) {
    return s;
  }
  return NewWritableFile(fname, result, options);
}

Status Env::GetChildrenFileAttributes(const std::string& dir,
                                      std::vector<FileAttributes>* result) {
  assert(result != nullptr);
  std::vector<std::string> child_fnames;
  Status s = GetChildren(dir, &child_fnames);
  if (!s.ok()) {
    return s;
  }
  result->resize(child_fnames.size());
  size_t result_size = 0;
  for (size_t i = 0; i < child_fnames.size(); ++i) {
    const std::string path = dir + "/" + child_fnames[i];
    if (!(s = GetFileSize(path, &(*result)[result_size].size_bytes)).ok()) {
      if (FileExists(path).IsNotFound()) {
        // The file may have been deleted since we listed the directory
        continue;
      }
      return s;
    }
    (*result)[result_size].name = std::move(child_fnames[i]);
    result_size++;
  }
  result->resize(result_size);
  return Status::OK();
}

SequentialFile::~SequentialFile() {
}

RandomAccessFile::~RandomAccessFile() {
}

WritableFile::~WritableFile() {
}

MemoryMappedFileBuffer::~MemoryMappedFileBuffer() {}

Logger::~Logger() {}

Status Logger::Close() {
  if (!closed_) {
    closed_ = true;
    return CloseImpl();
  } else {
    return Status::OK();
  }
}

Status Logger::CloseImpl() { return Status::NotSupported(); }

FileLock::~FileLock() {
}

void LogFlush(Logger *info_log) {
  if (info_log) {
    info_log->Flush();
  }
}

static void Logv(Logger *info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Log(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(info_log, format, ap);
  va_end(ap);
}

void Logger::Logv(const InfoLogLevel log_level, const char* format, va_list ap) {
  static const char* kInfoLogLevelNames[5] = { "DEBUG", "INFO", "WARN",
    "ERROR", "FATAL" };
  if (log_level < log_level_) {
    return;
  }

  if (log_level == InfoLogLevel::INFO_LEVEL) {
    // Doesn't print log level if it is INFO level.
    // This is to avoid unexpected performance regression after we add
    // the feature of log level. All the logs before we add the feature
    // are INFO level. We don't want to add extra costs to those existing
    // logging.
    Logv(format, ap);
  } else if (log_level == InfoLogLevel::HEADER_LEVEL) {
    LogHeader(format, ap);
  } else {
    char new_format[500];
    snprintf(new_format, sizeof(new_format) - 1, "[%s] %s",
      kInfoLogLevelNames[log_level], format);
    Logv(new_format, ap);
  }
}

static void Logv(const InfoLogLevel log_level, Logger *info_log, const char *format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= log_level) {
    if (log_level == InfoLogLevel::HEADER_LEVEL) {
      info_log->LogHeader(format, ap);
    } else {
      info_log->Logv(log_level, format, ap);
    }
  }
}

void Log(const InfoLogLevel log_level, Logger* info_log, const char* format,
         ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log, format, ap);
  va_end(ap);
}

static void Headerv(Logger *info_log, const char *format, va_list ap) {
  if (info_log) {
    info_log->LogHeader(format, ap);
  }
}

void Header(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log, format, ap);
  va_end(ap);
}

static void Debugv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    info_log->Logv(InfoLogLevel::DEBUG_LEVEL, format, ap);
  }
}

void Debug(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log, format, ap);
  va_end(ap);
}

static void Infov(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
    info_log->Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }
}

void Info(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log, format, ap);
  va_end(ap);
}

static void Warnv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::WARN_LEVEL) {
    info_log->Logv(InfoLogLevel::WARN_LEVEL, format, ap);
  }
}

void Warn(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log, format, ap);
  va_end(ap);
}

static void Errorv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::ERROR_LEVEL) {
    info_log->Logv(InfoLogLevel::ERROR_LEVEL, format, ap);
  }
}

void Error(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log, format, ap);
  va_end(ap);
}

static void Fatalv(Logger* info_log, const char* format, va_list ap) {
  if (info_log && info_log->GetInfoLogLevel() <= InfoLogLevel::FATAL_LEVEL) {
    info_log->Logv(InfoLogLevel::FATAL_LEVEL, format, ap);
  }
}

void Fatal(Logger* info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log, format, ap);
  va_end(ap);
}

void LogFlush(const std::shared_ptr<Logger>& info_log) {
  LogFlush(info_log.get());
}

void Log(const InfoLogLevel log_level, const std::shared_ptr<Logger>& info_log,
         const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(log_level, info_log.get(), format, ap);
  va_end(ap);
}

void Header(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Headerv(info_log.get(), format, ap);
  va_end(ap);
}

void Debug(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Debugv(info_log.get(), format, ap);
  va_end(ap);
}

void Info(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Infov(info_log.get(), format, ap);
  va_end(ap);
}

void Warn(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Warnv(info_log.get(), format, ap);
  va_end(ap);
}

void Error(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Errorv(info_log.get(), format, ap);
  va_end(ap);
}

void Fatal(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Fatalv(info_log.get(), format, ap);
  va_end(ap);
}

void Log(const std::shared_ptr<Logger>& info_log, const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  Logv(info_log.get(), format, ap);
  va_end(ap);
}

Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname,
                         bool should_sync) {
  LegacyFileSystemWrapper lfsw(env);
  return WriteStringToFile(&lfsw, data, fname, should_sync);
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  LegacyFileSystemWrapper lfsw(env);
  return ReadFileToString(&lfsw, fname, data);
}

static std::unordered_map<std::string, OptionTypeInfo> env_target_type_info = {
#ifndef ROCKSDB_LITE
    {"target", OptionTypeInfo::AsCustomP<Env>(
                   0, OptionVerificationType::kByName, OptionTypeFlags::kNone)},
#endif  // !ROCKSDB_LITE
};

EnvWrapper::EnvWrapper(Env* t) : target_(t) {
  RegisterOptions("WrappedOptions", &target_, &env_target_type_info);
}

EnvWrapper::~EnvWrapper() {
}

Status EnvWrapper::ValidateOptions(const DBOptions& db_opts,
                                   const ColumnFamilyOptions& cf_opts) const {
  if (target_ != nullptr) {
    return Env::ValidateOptions(db_opts, cf_opts);
  } else {
    return Status::InvalidArgument("Missing target env:", Name());
  }
}

namespace {  // anonymous namespace

void AssignEnvOptions(EnvOptions* env_options, const DBOptions& options) {
  env_options->use_mmap_reads = options.allow_mmap_reads;
  env_options->use_mmap_writes = options.allow_mmap_writes;
  env_options->use_direct_reads = options.use_direct_reads;
  env_options->set_fd_cloexec = options.is_fd_close_on_exec;
  env_options->bytes_per_sync = options.bytes_per_sync;
  env_options->compaction_readahead_size = options.compaction_readahead_size;
  env_options->random_access_max_buffer_size =
      options.random_access_max_buffer_size;
  env_options->rate_limiter = options.rate_limiter.get();
  env_options->writable_file_max_buffer_size =
      options.writable_file_max_buffer_size;
  env_options->allow_fallocate = options.allow_fallocate;
  env_options->strict_bytes_per_sync = options.strict_bytes_per_sync;
  options.env->SanitizeEnvOptions(env_options);
}

}

EnvOptions Env::OptimizeForLogWrite(const EnvOptions& env_options,
                                    const DBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.bytes_per_sync = db_options.wal_bytes_per_sync;
  optimized_env_options.writable_file_max_buffer_size =
      db_options.writable_file_max_buffer_size;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForManifestWrite(const EnvOptions& env_options) const {
  return env_options;
}

EnvOptions Env::OptimizeForLogRead(const EnvOptions& env_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = false;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForManifestRead(const EnvOptions& env_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = false;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForCompactionTableWrite(
    const EnvOptions& env_options, const ImmutableDBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_writes =
      db_options.use_direct_io_for_flush_and_compaction;
  return optimized_env_options;
}

EnvOptions Env::OptimizeForCompactionTableRead(
    const EnvOptions& env_options, const ImmutableDBOptions& db_options) const {
  EnvOptions optimized_env_options(env_options);
  optimized_env_options.use_direct_reads = db_options.use_direct_reads;
  return optimized_env_options;
}

EnvOptions::EnvOptions(const DBOptions& options) {
  AssignEnvOptions(this, options);
}

EnvOptions::EnvOptions() {
  DBOptions options;
  AssignEnvOptions(this, options);
}

Status NewEnvLogger(const std::string& fname, Env* env,
                    std::shared_ptr<Logger>* result) {
  EnvOptions options;
  // TODO: Tune the buffer size.
  options.writable_file_max_buffer_size = 1024 * 1024;
  std::unique_ptr<WritableFile> writable_file;
  const auto status = env->NewWritableFile(fname, &writable_file, options);
  if (!status.ok()) {
    return status;
  }

  *result = std::make_shared<EnvLogger>(
      NewLegacyWritableFileWrapper(std::move(writable_file)), fname, options,
      env);
  return Status::OK();
}

const std::shared_ptr<FileSystem>& Env::GetFileSystem() const {
  return file_system_;
}

#ifdef OS_WIN
std::unique_ptr<Env> NewCompositeEnv(std::shared_ptr<FileSystem> fs) {
  return std::unique_ptr<Env>(new CompositeEnvWrapper(Env::Default(), fs));
}
#endif

}  // namespace ROCKSDB_NAMESPACE
