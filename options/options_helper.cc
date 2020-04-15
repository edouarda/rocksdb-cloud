//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "options/options_helper.h"

#include <cassert>
#include <cctype>
#include <cstdlib>
#include <unordered_set>
#include <vector>

#include "options/cf_options.h"
#include "options/db_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
ConfigOptions::ConfigOptions()
#ifndef ROCKSDB_LITE
    : registry(ObjectRegistry::NewInstance())
#endif
{
  env = Env::Default();
}

ConfigOptions::ConfigOptions(const DBOptions& db_opts)
    : info_log(db_opts.info_log), env(db_opts.env) {
#ifndef ROCKSDB_LITE
  registry = db_opts.object_registry;
#endif
}

ConfigOptions ConfigOptions::Embedded() const {
  ConfigOptions embedded = *this;
  embedded.delimiter = ";";
  return embedded;
}

Status ValidateOptions(const DBOptions& db_opts,
                       const ColumnFamilyOptions& cf_opts) {
  Status s;
#ifndef ROCKSDB_LITE
  auto db_cfg = DBOptionsAsConfigurable(db_opts, ConfigOptions());
  auto cf_cfg = CFOptionsAsConfigurable(cf_opts);
  s = db_cfg->ValidateOptions(db_opts, cf_opts);
  if (s.ok()) s = cf_cfg->ValidateOptions(db_opts, cf_opts);
#else
  s = cf_opts.table_factory->ValidateOptions(db_opts, cf_opts);
#endif
  return s;
}

DBOptions BuildDBOptions(const ImmutableDBOptions& immutable_db_options,
                         const MutableDBOptions& mutable_db_options) {
  DBOptions options;

  options.create_if_missing = immutable_db_options.create_if_missing;
  options.create_missing_column_families =
      immutable_db_options.create_missing_column_families;
  options.error_if_exists = immutable_db_options.error_if_exists;
  options.paranoid_checks = immutable_db_options.paranoid_checks;
  options.env = immutable_db_options.env;
  options.rate_limiter = immutable_db_options.rate_limiter;
  options.sst_file_manager = immutable_db_options.sst_file_manager;
  options.info_log = immutable_db_options.info_log;
  options.info_log_level = immutable_db_options.info_log_level;
  options.max_open_files = mutable_db_options.max_open_files;
  options.max_file_opening_threads =
      immutable_db_options.max_file_opening_threads;
  options.max_total_wal_size = mutable_db_options.max_total_wal_size;
  options.statistics = immutable_db_options.statistics;
  options.use_fsync = immutable_db_options.use_fsync;
  options.db_paths = immutable_db_options.db_paths;
  options.db_log_dir = immutable_db_options.db_log_dir;
  options.wal_dir = immutable_db_options.wal_dir;
  options.delete_obsolete_files_period_micros =
      mutable_db_options.delete_obsolete_files_period_micros;
  options.max_background_jobs = mutable_db_options.max_background_jobs;
  options.base_background_compactions =
      mutable_db_options.base_background_compactions;
  options.max_background_compactions =
      mutable_db_options.max_background_compactions;
  options.bytes_per_sync = mutable_db_options.bytes_per_sync;
  options.wal_bytes_per_sync = mutable_db_options.wal_bytes_per_sync;
  options.strict_bytes_per_sync = mutable_db_options.strict_bytes_per_sync;
  options.max_subcompactions = immutable_db_options.max_subcompactions;
  options.max_background_flushes = immutable_db_options.max_background_flushes;
  options.max_log_file_size = immutable_db_options.max_log_file_size;
  options.log_file_time_to_roll = immutable_db_options.log_file_time_to_roll;
  options.keep_log_file_num = immutable_db_options.keep_log_file_num;
  options.recycle_log_file_num = immutable_db_options.recycle_log_file_num;
  options.max_manifest_file_size = immutable_db_options.max_manifest_file_size;
  options.table_cache_numshardbits =
      immutable_db_options.table_cache_numshardbits;
  options.WAL_ttl_seconds = immutable_db_options.wal_ttl_seconds;
  options.WAL_size_limit_MB = immutable_db_options.wal_size_limit_mb;
  options.manifest_preallocation_size =
      immutable_db_options.manifest_preallocation_size;
  options.allow_mmap_reads = immutable_db_options.allow_mmap_reads;
  options.allow_mmap_writes = immutable_db_options.allow_mmap_writes;
  options.use_direct_reads = immutable_db_options.use_direct_reads;
  options.use_direct_io_for_flush_and_compaction =
      immutable_db_options.use_direct_io_for_flush_and_compaction;
  options.allow_fallocate = immutable_db_options.allow_fallocate;
  options.is_fd_close_on_exec = immutable_db_options.is_fd_close_on_exec;
  options.stats_dump_period_sec = mutable_db_options.stats_dump_period_sec;
  options.stats_persist_period_sec =
      mutable_db_options.stats_persist_period_sec;
  options.persist_stats_to_disk = immutable_db_options.persist_stats_to_disk;
  options.stats_history_buffer_size =
      mutable_db_options.stats_history_buffer_size;
  options.advise_random_on_open = immutable_db_options.advise_random_on_open;
  options.db_write_buffer_size = immutable_db_options.db_write_buffer_size;
  options.write_buffer_manager = immutable_db_options.write_buffer_manager;
  options.access_hint_on_compaction_start =
      immutable_db_options.access_hint_on_compaction_start;
  options.new_table_reader_for_compaction_inputs =
      immutable_db_options.new_table_reader_for_compaction_inputs;
  options.compaction_readahead_size =
      mutable_db_options.compaction_readahead_size;
  options.random_access_max_buffer_size =
      immutable_db_options.random_access_max_buffer_size;
  options.writable_file_max_buffer_size =
      mutable_db_options.writable_file_max_buffer_size;
  options.use_adaptive_mutex = immutable_db_options.use_adaptive_mutex;
  options.listeners = immutable_db_options.listeners;
  options.plugins = immutable_db_options.plugins;
  options.enable_thread_tracking = immutable_db_options.enable_thread_tracking;
  options.delayed_write_rate = mutable_db_options.delayed_write_rate;
  options.enable_pipelined_write = immutable_db_options.enable_pipelined_write;
  options.unordered_write = immutable_db_options.unordered_write;
  options.allow_concurrent_memtable_write =
      immutable_db_options.allow_concurrent_memtable_write;
  options.enable_write_thread_adaptive_yield =
      immutable_db_options.enable_write_thread_adaptive_yield;
  options.max_write_batch_group_size_bytes =
      immutable_db_options.max_write_batch_group_size_bytes;
  options.write_thread_max_yield_usec =
      immutable_db_options.write_thread_max_yield_usec;
  options.write_thread_slow_yield_usec =
      immutable_db_options.write_thread_slow_yield_usec;
  options.skip_stats_update_on_db_open =
      immutable_db_options.skip_stats_update_on_db_open;
  options.skip_checking_sst_file_sizes_on_db_open =
      immutable_db_options.skip_checking_sst_file_sizes_on_db_open;
  options.wal_recovery_mode = immutable_db_options.wal_recovery_mode;
  options.allow_2pc = immutable_db_options.allow_2pc;
  options.row_cache = immutable_db_options.row_cache;
#ifndef ROCKSDB_LITE
  options.wal_filter = immutable_db_options.wal_filter;
  options.object_registry = immutable_db_options.object_registry;
#endif  // ROCKSDB_LITE
  options.fail_if_options_file_error =
      immutable_db_options.fail_if_options_file_error;
  options.dump_malloc_stats = immutable_db_options.dump_malloc_stats;
  options.avoid_flush_during_recovery =
      immutable_db_options.avoid_flush_during_recovery;
  options.avoid_flush_during_shutdown =
      mutable_db_options.avoid_flush_during_shutdown;
  options.allow_ingest_behind =
      immutable_db_options.allow_ingest_behind;
  options.preserve_deletes =
      immutable_db_options.preserve_deletes;
  options.two_write_queues = immutable_db_options.two_write_queues;
  options.manual_wal_flush = immutable_db_options.manual_wal_flush;
  options.atomic_flush = immutable_db_options.atomic_flush;
  options.avoid_unnecessary_blocking_io =
      immutable_db_options.avoid_unnecessary_blocking_io;
  options.log_readahead_size = immutable_db_options.log_readahead_size;
  options.file_checksum_gen_factory =
      immutable_db_options.file_checksum_gen_factory;
  options.best_efforts_recovery = immutable_db_options.best_efforts_recovery;
  return options;
}

ColumnFamilyOptions BuildColumnFamilyOptions(
    const ColumnFamilyOptions& options,
    const MutableCFOptions& mutable_cf_options) {
  ColumnFamilyOptions cf_opts(options);

  // Memtable related options
  cf_opts.write_buffer_size = mutable_cf_options.write_buffer_size;
  cf_opts.max_write_buffer_number = mutable_cf_options.max_write_buffer_number;
  cf_opts.arena_block_size = mutable_cf_options.arena_block_size;
  cf_opts.memtable_prefix_bloom_size_ratio =
      mutable_cf_options.memtable_prefix_bloom_size_ratio;
  cf_opts.memtable_whole_key_filtering =
      mutable_cf_options.memtable_whole_key_filtering;
  cf_opts.memtable_huge_page_size = mutable_cf_options.memtable_huge_page_size;
  cf_opts.max_successive_merges = mutable_cf_options.max_successive_merges;
  cf_opts.inplace_update_num_locks =
      mutable_cf_options.inplace_update_num_locks;
  cf_opts.prefix_extractor = mutable_cf_options.prefix_extractor;

  // Compaction related options
  cf_opts.disable_auto_compactions =
      mutable_cf_options.disable_auto_compactions;
  cf_opts.soft_pending_compaction_bytes_limit =
      mutable_cf_options.soft_pending_compaction_bytes_limit;
  cf_opts.hard_pending_compaction_bytes_limit =
      mutable_cf_options.hard_pending_compaction_bytes_limit;
  cf_opts.level0_file_num_compaction_trigger =
      mutable_cf_options.level0_file_num_compaction_trigger;
  cf_opts.level0_slowdown_writes_trigger =
      mutable_cf_options.level0_slowdown_writes_trigger;
  cf_opts.level0_stop_writes_trigger =
      mutable_cf_options.level0_stop_writes_trigger;
  cf_opts.max_compaction_bytes = mutable_cf_options.max_compaction_bytes;
  cf_opts.target_file_size_base = mutable_cf_options.target_file_size_base;
  cf_opts.target_file_size_multiplier =
      mutable_cf_options.target_file_size_multiplier;
  cf_opts.max_bytes_for_level_base =
      mutable_cf_options.max_bytes_for_level_base;
  cf_opts.max_bytes_for_level_multiplier =
      mutable_cf_options.max_bytes_for_level_multiplier;
  cf_opts.ttl = mutable_cf_options.ttl;
  cf_opts.periodic_compaction_seconds =
      mutable_cf_options.periodic_compaction_seconds;

  cf_opts.max_bytes_for_level_multiplier_additional.clear();
  for (auto value :
       mutable_cf_options.max_bytes_for_level_multiplier_additional) {
    cf_opts.max_bytes_for_level_multiplier_additional.emplace_back(value);
  }

  cf_opts.compaction_options_fifo = mutable_cf_options.compaction_options_fifo;
  cf_opts.compaction_options_universal =
      mutable_cf_options.compaction_options_universal;

  // Misc options
  cf_opts.max_sequential_skip_in_iterations =
      mutable_cf_options.max_sequential_skip_in_iterations;
  cf_opts.paranoid_file_checks = mutable_cf_options.paranoid_file_checks;
  cf_opts.report_bg_io_stats = mutable_cf_options.report_bg_io_stats;
  cf_opts.compression = mutable_cf_options.compression;
  cf_opts.compression_opts = mutable_cf_options.compression_opts;
  cf_opts.bottommost_compression = mutable_cf_options.bottommost_compression;
  cf_opts.bottommost_compression_opts =
      mutable_cf_options.bottommost_compression_opts;
  cf_opts.sample_for_compression = mutable_cf_options.sample_for_compression;

  cf_opts.table_factory = options.table_factory;

  // TODO(yhchiang): find some way to handle the following derived options
  // * max_file_size

  return cf_opts;
}

std::map<CompactionStyle, std::string>
    OptionsHelper::compaction_style_to_string = {
        {kCompactionStyleLevel, "kCompactionStyleLevel"},
        {kCompactionStyleUniversal, "kCompactionStyleUniversal"},
        {kCompactionStyleFIFO, "kCompactionStyleFIFO"},
        {kCompactionStyleNone, "kCompactionStyleNone"}};

std::map<CompactionPri, std::string> OptionsHelper::compaction_pri_to_string = {
    {kByCompensatedSize, "kByCompensatedSize"},
    {kOldestLargestSeqFirst, "kOldestLargestSeqFirst"},
    {kOldestSmallestSeqFirst, "kOldestSmallestSeqFirst"},
    {kMinOverlappingRatio, "kMinOverlappingRatio"}};

std::map<CompactionStopStyle, std::string>
    OptionsHelper::compaction_stop_style_to_string = {
        {kCompactionStopStyleSimilarSize, "kCompactionStopStyleSimilarSize"},
        {kCompactionStopStyleTotalSize, "kCompactionStopStyleTotalSize"}};

std::unordered_map<std::string, ChecksumType>
    OptionsHelper::checksum_type_string_map = {{"kNoChecksum", kNoChecksum},
                                               {"kCRC32c", kCRC32c},
                                               {"kxxHash", kxxHash},
                                               {"kxxHash64", kxxHash64}};

std::unordered_map<std::string, CompressionType>
    OptionsHelper::compression_type_string_map = {
        {"kNoCompression", kNoCompression},
        {"kSnappyCompression", kSnappyCompression},
        {"kZlibCompression", kZlibCompression},
        {"kBZip2Compression", kBZip2Compression},
        {"kLZ4Compression", kLZ4Compression},
        {"kLZ4HCCompression", kLZ4HCCompression},
        {"kXpressCompression", kXpressCompression},
        {"kZSTD", kZSTD},
        {"kZSTDNotFinalCompression", kZSTDNotFinalCompression},
        {"kDisableCompressionOption", kDisableCompressionOption}};
#ifndef ROCKSDB_LITE

bool ParseSliceTransformHelper(
    const std::string& kFixedPrefixName, const std::string& kCappedPrefixName,
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  const char* no_op_name = "rocksdb.Noop";
  size_t no_op_length = strlen(no_op_name);
  auto& pe_value = value;
  if (pe_value.size() > kFixedPrefixName.size() &&
      pe_value.compare(0, kFixedPrefixName.size(), kFixedPrefixName) == 0) {
    int prefix_length = ParseInt(trim(value.substr(kFixedPrefixName.size())));
    slice_transform->reset(NewFixedPrefixTransform(prefix_length));
  } else if (pe_value.size() > kCappedPrefixName.size() &&
             pe_value.compare(0, kCappedPrefixName.size(), kCappedPrefixName) ==
                 0) {
    int prefix_length =
        ParseInt(trim(pe_value.substr(kCappedPrefixName.size())));
    slice_transform->reset(NewCappedPrefixTransform(prefix_length));
  } else if (pe_value.size() == no_op_length &&
             pe_value.compare(0, no_op_length, no_op_name) == 0) {
    const SliceTransform* no_op_transform = NewNoopTransform();
    slice_transform->reset(no_op_transform);
  } else if (value == kNullptrString) {
    slice_transform->reset();
  } else {
    return false;
  }

  return true;
}

bool ParseSliceTransform(
    const std::string& value,
    std::shared_ptr<const SliceTransform>* slice_transform) {
  // While we normally don't convert the string representation of a
  // pointer-typed option into its instance, here we do so for backward
  // compatibility as we allow this action in SetOption().

  // TODO(yhchiang): A possible better place for these serialization /
  // deserialization is inside the class definition of pointer-typed
  // option itself, but this requires a bigger change of public API.
  bool result =
      ParseSliceTransformHelper("fixed:", "capped:", value, slice_transform);
  if (result) {
    return result;
  }
  result = ParseSliceTransformHelper(
      "rocksdb.FixedPrefix.", "rocksdb.CappedPrefix.", value, slice_transform);
  if (result) {
    return result;
  }
  // TODO(yhchiang): we can further support other default
  //                 SliceTransforms here.
  return false;
}

static bool ParseOptionHelper(char* opt_address, const OptionType& opt_type,
                              const std::string& value) {
  switch (opt_type) {
    case OptionType::kBoolean:
      *reinterpret_cast<bool*>(opt_address) = ParseBoolean("", value);
      break;
    case OptionType::kInt:
      *reinterpret_cast<int*>(opt_address) = ParseInt(value);
      break;
    case OptionType::kInt32T:
      *reinterpret_cast<int32_t*>(opt_address) = ParseInt32(value);
      break;
    case OptionType::kInt64T:
      PutUnaligned(reinterpret_cast<int64_t*>(opt_address), ParseInt64(value));
      break;
    case OptionType::kUInt:
      *reinterpret_cast<unsigned int*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt32T:
      *reinterpret_cast<uint32_t*>(opt_address) = ParseUint32(value);
      break;
    case OptionType::kUInt64T:
      PutUnaligned(reinterpret_cast<uint64_t*>(opt_address), ParseUint64(value));
      break;
    case OptionType::kSizeT:
      PutUnaligned(reinterpret_cast<size_t*>(opt_address), ParseSizeT(value));
      break;
    case OptionType::kString:
      *reinterpret_cast<std::string*>(opt_address) = value;
      break;
    case OptionType::kDouble:
      *reinterpret_cast<double*>(opt_address) = ParseDouble(value);
      break;
    case OptionType::kCompactionStyle:
      return ParseEnum<CompactionStyle>(
          compaction_style_string_map, value,
          reinterpret_cast<CompactionStyle*>(opt_address));
    case OptionType::kCompactionPri:
      return ParseEnum<CompactionPri>(
          compaction_pri_string_map, value,
          reinterpret_cast<CompactionPri*>(opt_address));
    case OptionType::kCompressionType:
      return ParseEnum<CompressionType>(
          compression_type_string_map, value,
          reinterpret_cast<CompressionType*>(opt_address));
    case OptionType::kSliceTransform:
      return ParseSliceTransform(
          value, reinterpret_cast<std::shared_ptr<const SliceTransform>*>(
                     opt_address));
    case OptionType::kChecksumType:
      return ParseEnum<ChecksumType>(
          checksum_type_string_map, value,
          reinterpret_cast<ChecksumType*>(opt_address));
    case OptionType::kEncodingType:
      return ParseEnum<EncodingType>(
          encoding_type_string_map, value,
          reinterpret_cast<EncodingType*>(opt_address));
    case OptionType::kCompactionStopStyle:
      return ParseEnum<CompactionStopStyle>(
          compaction_stop_style_string_map, value,
          reinterpret_cast<CompactionStopStyle*>(opt_address));
    default:
      return false;
  }
  return true;
}

bool SerializeSingleOptionHelper(const char* opt_address,
                                 const OptionType opt_type,
                                 std::string* value) {

  assert(value);
  switch (opt_type) {
    case OptionType::kBoolean:
      *value = *(reinterpret_cast<const bool*>(opt_address)) ? "true" : "false";
      break;
    case OptionType::kInt:
      *value = ToString(*(reinterpret_cast<const int*>(opt_address)));
      break;
    case OptionType::kInt32T:
      *value = ToString(*(reinterpret_cast<const int32_t*>(opt_address)));
      break;
    case OptionType::kInt64T:
      {
        int64_t v;
        GetUnaligned(reinterpret_cast<const int64_t*>(opt_address), &v);
        *value = ToString(v);
      }
      break;
    case OptionType::kUInt:
      *value = ToString(*(reinterpret_cast<const unsigned int*>(opt_address)));
      break;
    case OptionType::kUInt32T:
      *value = ToString(*(reinterpret_cast<const uint32_t*>(opt_address)));
      break;
    case OptionType::kUInt64T:
      {
        uint64_t v;
        GetUnaligned(reinterpret_cast<const uint64_t*>(opt_address), &v);
        *value = ToString(v);
      }
      break;
    case OptionType::kSizeT:
      {
        size_t v;
        GetUnaligned(reinterpret_cast<const size_t*>(opt_address), &v);
        *value = ToString(v);
      }
      break;
    case OptionType::kDouble:
      *value = ToString(*(reinterpret_cast<const double*>(opt_address)));
      break;
    case OptionType::kString:
      *value = EscapeOptionString(
          *(reinterpret_cast<const std::string*>(opt_address)));
      break;
    case OptionType::kCompactionStyle:
      return SerializeEnum<CompactionStyle>(
          compaction_style_string_map,
          *(reinterpret_cast<const CompactionStyle*>(opt_address)), value);
    case OptionType::kCompactionPri:
      return SerializeEnum<CompactionPri>(
          compaction_pri_string_map,
          *(reinterpret_cast<const CompactionPri*>(opt_address)), value);
    case OptionType::kCompressionType:
      return SerializeEnum<CompressionType>(
          compression_type_string_map,
          *(reinterpret_cast<const CompressionType*>(opt_address)), value);
    case OptionType::kSliceTransform: {
      const auto* slice_transform_ptr =
          reinterpret_cast<const std::shared_ptr<const SliceTransform>*>(
              opt_address);
      *value = slice_transform_ptr->get() ? slice_transform_ptr->get()->Name()
                                          : kNullptrString;
      break;
    }
    case OptionType::kChecksumType:
      return SerializeEnum<ChecksumType>(
          checksum_type_string_map,
          *reinterpret_cast<const ChecksumType*>(opt_address), value);
    case OptionType::kEncodingType:
      return SerializeEnum<EncodingType>(
          encoding_type_string_map,
          *reinterpret_cast<const EncodingType*>(opt_address), value);
    case OptionType::kCompactionStopStyle:
      return SerializeEnum<CompactionStopStyle>(
          compaction_stop_style_string_map,
          *reinterpret_cast<const CompactionStopStyle*>(opt_address), value);
    default:
      return false;
  }
  return true;
}

template <typename T>
Status ConfigureFromMap(
    Configurable* config,
    const std::unordered_map<std::string, std::string>& opts,
    const std::string& name, const ConfigOptions& cfg, T* new_opts) {
  Status s = config->ConfigureFromMap(opts, cfg);
  if (s.ok()) {
    *new_opts = *(config->GetOptions<T>(name));
  }
  return s;
}

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* /*info_log*/, MutableCFOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  ConfigOptions parse_opts;

  const auto config = CFOptionsAsConfigurable(base_options);
  return ConfigureFromMap<MutableCFOptions>(
      config.get(), options_map, OptionsHelper::kMutableCFOptionsName,
      parse_opts, new_options);
}

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options) {
  assert(new_options);
  *new_options = base_options;
  ConfigOptions opts;
  auto config = DBOptionsAsConfigurable(base_options);
  return ConfigureFromMap<MutableDBOptions>(
      config.get(), options_map, OptionsHelper::kMutableDBOptionsName, opts,
      new_options);
}

Status StringToMap(const std::string& opts_str,
                   std::unordered_map<std::string, std::string>* opts_map) {
  assert(opts_map);
  // Example:
  //   opts_str = "write_buffer_size=1024;max_write_buffer_number=2;"
  //              "nested_opt={opt1=1;opt2=2};max_bytes_for_level_base=100"
  size_t pos = 0;
  std::string opts = trim(opts_str);
  // If the input string starts and ends with "{...}", strip off the brackets
  while (opts.size() > 2 && opts[0] == '{' && opts[opts.size() - 1] == '}') {
    opts = trim(opts.substr(1, opts.size() - 2));
  }

  while (pos < opts.size()) {
    size_t eq_pos = opts.find('=', pos);
    if (eq_pos == std::string::npos) {
      return Status::InvalidArgument("Mismatched key value pair, '=' expected");
    }
    std::string key = trim(opts.substr(pos, eq_pos - pos));
    if (key.empty()) {
      return Status::InvalidArgument("Empty key found");
    }

    std::string value;
    Status s = OptionTypeInfo::NextToken(opts, ';', eq_pos + 1, &pos, &value);
    if (!s.ok()) {
      return s;
    } else {
      (*opts_map)[key] = value;
      if (pos == std::string::npos) {
        break;
      } else {
        pos++;
      }
    }
  }

  return Status::OK();
}

Status GetStringFromMutableDBOptions(const MutableDBOptions& mutable_opts,
                                     const ConfigOptions& cfg_opts,
                                     std::string* opt_string) {
  auto config = DBOptionsAsConfigurable(mutable_opts);
  return config->GetOptionString(cfg_opts, opt_string);
}

Status GetStringFromDBOptions(std::string* opt_string,
                              const DBOptions& db_options,
                              const std::string& delimiter) {
  ConfigOptions options(db_options);
  options.delimiter = delimiter;
  return GetStringFromDBOptions(db_options, options, opt_string);
}

Status GetStringFromDBOptions(const DBOptions& db_options,
                              const ConfigOptions& options,
                              std::string* opt_string) {
  assert(opt_string);
  opt_string->clear();
  auto config = DBOptionsAsConfigurable(db_options, options);
  return config->GetOptionString(options, opt_string);
}

Status GetStringFromMutableCFOptions(const MutableCFOptions& mutable_opts,
                                     const ConfigOptions& cfg_opts,
                                     std::string* opt_string) {
  assert(opt_string);
  opt_string->clear();
  const auto config = CFOptionsAsConfigurable(mutable_opts);
  return config->GetOptionString(cfg_opts, opt_string);
}

Status GetStringFromColumnFamilyOptions(std::string* opt_string,
                                        const ColumnFamilyOptions& cf_options,
                                        const std::string& delimiter) {
  ConfigOptions options;
  options.delimiter = delimiter;
  return GetStringFromColumnFamilyOptions(cf_options, options, opt_string);
}

Status GetStringFromColumnFamilyOptions(const ColumnFamilyOptions& cf_options,
                                        const ConfigOptions& options,
                                        std::string* opt_string) {
  const auto config = CFOptionsAsConfigurable(cf_options);
  return config->GetOptionString(options, opt_string);
}

Status GetStringFromCompressionType(std::string* compression_str,
                                    CompressionType compression_type) {
  bool ok = SerializeEnum<CompressionType>(compression_type_string_map,
                                           compression_type, compression_str);
  if (ok) {
    return Status::OK();
  } else {
    return Status::InvalidArgument("Invalid compression types");
  }
}

std::vector<CompressionType> GetSupportedCompressions() {
  std::vector<CompressionType> supported_compressions;
  for (const auto& comp_to_name : compression_type_string_map) {
    CompressionType t = comp_to_name.second;
    if (t != kDisableCompressionOption && CompressionTypeSupported(t)) {
      supported_compressions.push_back(t);
    }
  }
  return supported_compressions;
}


Status GetColumnFamilyOptionsFromMap(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    ColumnFamilyOptions* new_options, bool input_strings_escaped,
    bool ignore_unknown_options) {
  ConfigOptions cfg_options;
  cfg_options.ignore_unknown_options = ignore_unknown_options;
  cfg_options.input_strings_escaped = input_strings_escaped;
  return GetColumnFamilyOptionsFromMap(base_options, opts_map, cfg_options,
                                       new_options);
}

Status GetColumnFamilyOptionsFromMap(
    const ColumnFamilyOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    const ConfigOptions& cfg_options, ColumnFamilyOptions* new_options) {
  assert(new_options);

  std::unordered_map<std::string, std::string> unused;
  ConfigOptions copy = cfg_options;
  copy.ignore_unknown_options = true;

  *new_options = base_options;

  const auto config = CFOptionsAsConfigurable(base_options);
  return ConfigureFromMap<ColumnFamilyOptions>(config.get(), opts_map,
                                               OptionsHelper::kCFOptionsName,
                                               cfg_options, new_options);
}

Status GetColumnFamilyOptionsFromString(
    const ColumnFamilyOptions& base_options,
    const std::string& opts_str,
    ColumnFamilyOptions* new_options) {
  ConfigOptions cfg_options;
  cfg_options.input_strings_escaped = false;
  cfg_options.ignore_unknown_options = false;
  return GetColumnFamilyOptionsFromString(base_options, opts_str, cfg_options,
                                          new_options);
}

Status GetColumnFamilyOptionsFromString(const ColumnFamilyOptions& base_options,
                                        const std::string& opts_str,
                                        const ConfigOptions& cfg_options,
                                        ColumnFamilyOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    *new_options = base_options;
    return s;
  }
  return GetColumnFamilyOptionsFromMap(base_options, opts_map, cfg_options,
                                       new_options);
}

Status GetDBOptionsFromMap(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    DBOptions* new_options, bool input_strings_escaped,
    bool ignore_unknown_options) {
  ConfigOptions cfg_options(base_options);
  cfg_options.input_strings_escaped = input_strings_escaped;
  cfg_options.ignore_unknown_options = ignore_unknown_options;
  return GetDBOptionsFromMap(base_options, opts_map, cfg_options, new_options);
}

Status GetDBOptionsFromMap(
    const DBOptions& base_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    const ConfigOptions& cfg_options, DBOptions* new_options) {
  assert(new_options);
  *new_options = base_options;

  // We need to make a copy of the ConfigOptions as they might
  // change if we are loading things related to it (registry, env).
  ConfigOptions copy = cfg_options;
  copy.registry = cfg_options.registry->Clone();
  auto config = DBOptionsAsConfigurable(base_options, copy);
  return ConfigureFromMap<DBOptions>(
      config.get(), opts_map, OptionsHelper::kDBOptionsName, copy, new_options);
}

Status GetDBOptionsFromString(const DBOptions& base_options,
                              const std::string& opts_str,
                              DBOptions* new_options) {
  ConfigOptions cfg_options(base_options);
  cfg_options.input_strings_escaped = false;
  cfg_options.ignore_unknown_options = false;

  return GetDBOptionsFromString(base_options, opts_str, cfg_options,
                                new_options);
}

Status GetDBOptionsFromString(const DBOptions& base_options,
                              const std::string& opts_str,
                              const ConfigOptions& cfg_options,
                              DBOptions* new_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    *new_options = base_options;
    return s;
  }
  return GetDBOptionsFromMap(base_options, opts_map, cfg_options, new_options);
}

Status GetOptionsFromString(const Options& base_options,
                            const std::string& opts_str, Options* new_options) {
  ConfigOptions cfg_options(base_options);
  cfg_options.input_strings_escaped = false;
  cfg_options.ignore_unknown_options = false;

  return GetOptionsFromString(base_options, opts_str, cfg_options, new_options);
}

Status GetOptionsFromString(const Options& base_options,
                            const std::string& opts_str,
                            const ConfigOptions& cfg_options,
                            Options* new_options) {
  assert(new_options);
  *new_options = base_options;

  ColumnFamilyOptions new_cf_options;
  std::unordered_map<std::string, std::string> unused_opts;
  std::unordered_map<std::string, std::string> opts_map;

  // We need to make a copy of the ConfigOptions as they might
  // change if we are loading things related to it (registry, env).
  ConfigOptions copy = cfg_options;
  copy.ignore_unknown_options = true;
  copy.registry = cfg_options.registry->Clone();

  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }
  auto config = DBOptionsAsConfigurable(base_options, copy);
  s = config->ConfigureFromMap(opts_map, copy, &unused_opts);
  if (s.ok()) {
    DBOptions* new_db_options =
        config->GetOptions<DBOptions>(OptionsHelper::kDBOptionsName);
    copy.ignore_unknown_options = cfg_options.ignore_unknown_options;
    copy.registry = new_db_options->object_registry;

    s = GetColumnFamilyOptionsFromMap(base_options, unused_opts, copy,
                                      &new_cf_options);
    if (s.ok()) {
      *new_options = Options(*new_db_options, new_cf_options);
    }
  }
  return s;
}

std::unordered_map<std::string, EncodingType>
    OptionsHelper::encoding_type_string_map = {{"kPlain", kPlain},
                                               {"kPrefix", kPrefix}};

std::unordered_map<std::string, CompactionStyle>
    OptionsHelper::compaction_style_string_map = {
        {"kCompactionStyleLevel", kCompactionStyleLevel},
        {"kCompactionStyleUniversal", kCompactionStyleUniversal},
        {"kCompactionStyleFIFO", kCompactionStyleFIFO},
        {"kCompactionStyleNone", kCompactionStyleNone}};

std::unordered_map<std::string, CompactionPri>
    OptionsHelper::compaction_pri_string_map = {
        {"kByCompensatedSize", kByCompensatedSize},
        {"kOldestLargestSeqFirst", kOldestLargestSeqFirst},
        {"kOldestSmallestSeqFirst", kOldestSmallestSeqFirst},
        {"kMinOverlappingRatio", kMinOverlappingRatio}};

std::unordered_map<std::string, CompactionStopStyle>
    OptionsHelper::compaction_stop_style_string_map = {
        {"kCompactionStopStyleSimilarSize", kCompactionStopStyleSimilarSize},
        {"kCompactionStopStyleTotalSize", kCompactionStopStyleTotalSize}};

Status OptionTypeInfo::NextToken(const std::string& opts, char delimiter,
                                 size_t pos, size_t* end, std::string* token) {
  while (pos < opts.size() && isspace(opts[pos])) {
    ++pos;
  }
  // Empty value at the end
  if (pos >= opts.size()) {
    *token = "";
    *end = std::string::npos;
    return Status::OK();
  } else if (opts[pos] == '{') {
    int count = 1;
    size_t brace_pos = pos + 1;
    while (brace_pos < opts.size()) {
      if (opts[brace_pos] == '{') {
        ++count;
      } else if (opts[brace_pos] == '}') {
        --count;
        if (count == 0) {
          break;
        }
      }
      ++brace_pos;
    }
    // found the matching closing brace
    if (count == 0) {
      *token = trim(opts.substr(pos + 1, brace_pos - pos - 1));
      // skip all whitespace and move to the next delimiter
      // brace_pos points to the next position after the matching '}'
      pos = brace_pos + 1;
      while (pos < opts.size() && isspace(opts[pos])) {
        ++pos;
      }
      if (pos < opts.size() && opts[pos] != delimiter) {
        return Status::InvalidArgument("Unexpected chars after nested options");
      }
      *end = pos;
    } else {
      return Status::InvalidArgument(
          "Mismatched curly braces for nested options");
    }
  } else {
    *end = opts.find(delimiter, pos);
    if (*end == std::string::npos) {
      // It either ends with a trailing semi-colon or the last key-value pair
      *token = trim(opts.substr(pos));
    } else {
      *token = trim(opts.substr(pos, *end - pos));
    }
  }
  return Status::OK();
}

Status OptionTypeInfo::ParseOption(const std::string& opt_name,
                                   const std::string& value,
                                   const ConfigOptions& options,
                                   void* opt_ptr) const {
  if (IsDeprecated()) {
    return Status::OK();
  }
  try {
    char* opt_addr = reinterpret_cast<char*>(opt_ptr) + offset;
    const std::string& opt_value =
        options.input_strings_escaped ? UnescapeOptionString(value) : value;

    if (opt_addr == nullptr) {
      return Status::NotFound("Could not find option: ", opt_name);
    } else if (parser_func != nullptr) {
        if (IsEnabled(OptionTypeFlags::kDontPrepare)) {
          ConfigOptions copy = options;
          copy.invoke_prepare_options = false;
          return parser_func(opt_name, opt_value, copy, opt_addr);
        } else {
          return parser_func(opt_name, opt_value, options, opt_addr);
        }
    } else if (ParseOptionHelper(opt_addr, type, opt_value)) {
      return Status::OK();
    } else if (IsConfigurable()) {
      // The option is <config>.<name>
      Configurable* config = AsRawPointer<Configurable>(opt_ptr);
      if (opt_value.empty()) {
        return Status::OK();
      } else if (config == nullptr) {
        return Status::NotFound("Could not find configurable: ", opt_name);
      } else if (opt_value.find("=") != std::string::npos) {
        ConfigOptions copy = options;
        copy.ignore_unknown_options = false;
        if (IsEnabled(OptionTypeFlags::kDontPrepare)) {
          //copy.invoke_prepare_options = false;
        }
        return config->ConfigureFromString(opt_value, copy);
      } else {
        return config->ConfigureOption(opt_name, opt_value, options);
      }
    } else if (IsByName()) {
      return Status::NotSupported("Deserializing the option " + opt_name +
                                  " is not supported");
    } else {
      return Status::InvalidArgument("Error parsing:", opt_name);
    }
  } catch (std::exception& e) {
    return Status::InvalidArgument("Error parsing " + opt_name + ":" +
                                   std::string(e.what()));
  }
}

Status OptionTypeInfo::ParseStruct(
    const std::string& struct_name,
    const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
    const std::string& opt_name, const std::string& opt_value,
    const ConfigOptions& options, char* opt_addr) {
  assert(struct_map);
  Status status;
  if (opt_name == struct_name || EndsWith(opt_name, "." + struct_name)) {
    // This option represents the entire struct
    std::unordered_map<std::string, std::string> opt_map;
    status = StringToMap(opt_value, &opt_map);
    if (status.ok()) {
      for (const auto& map_iter : opt_map) {
        const auto iter = struct_map->find(map_iter.first);
        if (iter != struct_map->end()) {
          status = iter->second.ParseOption(map_iter.first, map_iter.second,
                                            options, opt_addr);
        } else {
          return Status::InvalidArgument("Unrecognized option: ",
                                         struct_name + "." + map_iter.first);
        }
      }
    }
  } else if (StartsWith(opt_name, struct_name + ".")) {
    // This option represents a nested field in the struct (e.g, struct.field)
    std::string elem_name;
    const auto opt_info = FindOption(opt_name.substr(struct_name.size() + 1),
                                     *struct_map, &elem_name);
    if (opt_info != nullptr) {
      status = opt_info->ParseOption(elem_name, opt_value, options, opt_addr);
    } else {
      status = Status::InvalidArgument("Unrecognized option: ", opt_name);
    }
  } else {
    // This option represents a field in the struct (e.g. field)
    std::string elem_name;
    const auto opt_info = FindOption(opt_name, *struct_map, &elem_name);
    if (opt_info != nullptr) {
      status = opt_info->ParseOption(elem_name, opt_value, options, opt_addr);
    } else {
      status = Status::InvalidArgument("Unrecognized option: ",
                                       struct_name + "." + opt_name);
    }
  }
  return status;
}

Status OptionTypeInfo::SerializeOption(const std::string& opt_name,
                                       const void* const opt_ptr,
                                       const ConfigOptions& options,
                                       std::string* opt_value) const {
  // If the option is no longer used in rocksdb and marked as deprecated,
  // we skip it in the serialization.
  Status s;
  const char* opt_addr = reinterpret_cast<const char*>(opt_ptr) + offset;
  if (opt_addr == nullptr || IsDeprecated()) {
    return Status::OK();
  } else if (IsEnabled(OptionTypeFlags::kStringNone)) {
    s = Status::NotSupported("Cannot serialize option: ", opt_name);
  } else if (string_func != nullptr) {
    return string_func(opt_name, opt_addr, options, opt_value);
  } else if (SerializeSingleOptionHelper(opt_addr, type, opt_value)) {
    s = Status::OK();
  } else if (IsCustomizable()) {
    const Customizable* custom = AsRawPointer<Customizable>(opt_ptr);
    if (custom == nullptr) {
      *opt_value = kNullptrString;
    } else if (IsEnabled(OptionTypeFlags::kStringShallow) &&
               !options.IsDetailed()) {
      *opt_value = custom->GetId();
    } else {
      *opt_value = custom->ToString(options.Embedded());
    }
    return Status::OK();
  } else if (IsConfigurable()) {
    const Configurable* config = AsRawPointer<Configurable>(opt_ptr);
    if (config != nullptr) {
      *opt_value = config->ToString(options.Embedded());
    }
    return Status::OK();
  } else {
    s = Status::InvalidArgument("Cannot serialize option: ", opt_name);
  }
  return s;
}

Status OptionTypeInfo::SerializeStruct(
    const std::string& struct_name,
    const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
    const std::string& opt_name, const char* opt_addr,
    const ConfigOptions& opts, std::string* value) {
  assert(struct_map);
  Status status;
  if (EndsWith(opt_name, struct_name)) {
    ConfigOptions embedded = opts.Embedded();
    // This option represents the entire struct
    std::string result;
    for (const auto& iter : *struct_map) {
      std::string single;
      const auto& opt_info = iter.second;
      if (opt_info.ShouldSerialize()) {
        status =
            opt_info.SerializeOption(iter.first, opt_addr, embedded, &single);
        if (!status.ok()) {
          return status;
        } else {
          result.append(iter.first + "=" + single + embedded.delimiter);
        }
      }
    }
    *value = "{" + result + "}";
  } else if (StartsWith(opt_name, struct_name + ".")) {
    // This option represents a nested field in the struct (e.g, struct.field)
    std::string elem_name;
    const auto opt_info = FindOption(opt_name.substr(struct_name.size() + 1),
                                     *struct_map, &elem_name);
    if (opt_info != nullptr) {
      status = opt_info->SerializeOption(elem_name, opt_addr, opts, value);
    } else {
      status = Status::InvalidArgument("Unrecognized option: ", opt_name);
    }
  } else {
    // This option represents a field in the struct (e.g. field)
    std::string elem_name;
    const auto opt_info = FindOption(opt_name, *struct_map, &elem_name);
    if (opt_info == nullptr) {
      return Status::InvalidArgument("Unrecognized option: ", opt_name);
    } else if (opt_info->ShouldSerialize()) {
      return opt_info->SerializeOption(opt_name + "." + elem_name, opt_addr,
                                       opts, value);
    }
  }
  return Status::OK();
}

template <typename T>
bool IsOptionEqual(const char* offset1, const char* offset2) {
  return (*reinterpret_cast<const T*>(offset1) ==
          *reinterpret_cast<const T*>(offset2));
}

static bool AreEqualDoubles(const double a, const double b) {
  return (fabs(a - b) < 0.00001);
}

static bool AreOptionsEqual(OptionType type, const char* this_offset,
                            const char* that_offset) {
  switch (type) {
    case OptionType::kBoolean:
      return IsOptionEqual<bool>(this_offset, that_offset);
    case OptionType::kInt:
      return IsOptionEqual<int>(this_offset, that_offset);
    case OptionType::kUInt:
      return IsOptionEqual<unsigned int>(this_offset, that_offset);
    case OptionType::kInt32T:
      return IsOptionEqual<int32_t>(this_offset, that_offset);
    case OptionType::kInt64T: {
      int64_t v1, v2;
      GetUnaligned(reinterpret_cast<const int64_t*>(this_offset), &v1);
      GetUnaligned(reinterpret_cast<const int64_t*>(that_offset), &v2);
      return (v1 == v2);
    }
    case OptionType::kUInt32T:
      return IsOptionEqual<uint32_t>(this_offset, that_offset);
    case OptionType::kUInt64T: {
      uint64_t v1, v2;
      GetUnaligned(reinterpret_cast<const uint64_t*>(this_offset), &v1);
      GetUnaligned(reinterpret_cast<const uint64_t*>(that_offset), &v2);
      return (v1 == v2);
    }
    case OptionType::kSizeT: {
      size_t v1, v2;
      GetUnaligned(reinterpret_cast<const size_t*>(this_offset), &v1);
      GetUnaligned(reinterpret_cast<const size_t*>(that_offset), &v2);
      return (v1 == v2);
    }
    case OptionType::kString:
      return IsOptionEqual<std::string>(this_offset, that_offset);
    case OptionType::kDouble:
      return AreEqualDoubles(*reinterpret_cast<const double*>(this_offset),
                             *reinterpret_cast<const double*>(that_offset));
    case OptionType::kCompactionStyle:
      return IsOptionEqual<CompactionStyle>(this_offset, that_offset);
    case OptionType::kCompactionStopStyle:
      return IsOptionEqual<CompactionStopStyle>(this_offset, that_offset);
    case OptionType::kCompactionPri:
      return IsOptionEqual<CompactionPri>(this_offset, that_offset);
    case OptionType::kCompressionType:
      return IsOptionEqual<CompressionType>(this_offset, that_offset);
    case OptionType::kChecksumType:
      return IsOptionEqual<ChecksumType>(this_offset, that_offset);
    case OptionType::kEncodingType:
      return IsOptionEqual<EncodingType>(this_offset, that_offset);
    default:
      return false;
  }  // End switch
}

bool OptionTypeInfo::MatchesOption(const std::string& opt_name,
                                   const void* const this_ptr,
                                   const void* const that_ptr,
                                   const ConfigOptions& options,
                                   std::string* mismatch) const {
  auto level = GetSanityLevel();
  if (!options.IsCheckEnabled(level)) {
    return true;  // If the sanity level is not being checked, skip it
  }
  const auto this_addr = reinterpret_cast<const char*>(this_ptr) + offset;
  const auto that_addr = reinterpret_cast<const char*>(that_ptr) + offset;
  if (this_addr == nullptr || that_addr == nullptr) {
    if (this_addr == that_addr) {
      return true;
    }
  } else if (equals_func != nullptr) {
    if (equals_func(opt_name, this_addr, that_addr, options, mismatch)) {
      return true;
    }
  } else if (AreOptionsEqual(type, this_addr, that_addr)) {
    return true;
  } else if (IsConfigurable()) {
    const auto* this_config = AsRawPointer<Configurable>(this_ptr);
    const auto* that_config = AsRawPointer<Configurable>(that_ptr);
    if (this_config == that_config) {
      return true;
    } else if (this_config != nullptr && that_config != nullptr) {
      std::string bad_name;
      bool matches;
      if (level < options.sanity_level) {
        ConfigOptions copy = options;
        copy.sanity_level = level;
        matches = this_config->Matches(that_config, copy, &bad_name);
      } else {
        matches = this_config->Matches(that_config, options, &bad_name);
      }
      if (!matches) {
        *mismatch = opt_name + "." + bad_name;
      }
      return matches;
    }
  }
  if (mismatch->empty()) {
    *mismatch = opt_name;
  }
  return false;
}

bool OptionTypeInfo::MatchesStruct(
    const std::string& struct_name,
    const std::unordered_map<std::string, OptionTypeInfo>* struct_map,
    const std::string& opt_name, const char* this_offset,
    const char* that_offset, const ConfigOptions& opts, std::string* mismatch) {
  assert(struct_map);
  Status status;
  bool matches = true;
  std::string result;
  if (EndsWith(opt_name, struct_name)) {
    // This option represents the entire struct
    for (const auto& iter : *struct_map) {
      const auto& opt_info = iter.second;

      matches = opt_info.MatchesOption(iter.first, this_offset, that_offset,
                                       opts, &result);
      if (!matches) {
        *mismatch = struct_name + "." + result;
        return false;
      }
    }
  } else if (StartsWith(opt_name, struct_name + ".")) {
    // This option represents a nested field in the struct (e.g, struct.field)
    std::string elem_name;
    const auto opt_info = FindOption(opt_name.substr(struct_name.size() + 1),
                                     *struct_map, &elem_name);
    assert(opt_info);
    if (opt_info == nullptr) {
      *mismatch = opt_name;
      matches = false;
    } else if (!opt_info->MatchesOption(elem_name, this_offset, that_offset,
                                        opts, &result)) {
      matches = false;
      *mismatch = struct_name + "." + result;
    }
  } else {
    // This option represents a field in the struct (e.g. field)
    std::string elem_name;
    const auto opt_info = FindOption(opt_name, *struct_map, &elem_name);
    assert(opt_info);
    if (opt_info == nullptr) {
      *mismatch = struct_name + "." + opt_name;
      matches = false;
    } else if (!opt_info->MatchesOption(elem_name, this_offset, that_offset,
                                        opts, &result)) {
      matches = false;
      *mismatch = struct_name + "." + result;
    }
  }
  return matches;
}

bool MatchesOptionsTypeFromMap(
    const std::unordered_map<std::string, OptionTypeInfo>& type_map,
    const void* const this_ptr, const void* const that_ptr,
    const ConfigOptions& options, std::string* mismatch) {
  for (auto& pair : type_map) {
    // We skip checking deprecated variables as they might
    // contain random values since they might not be initialized
    if (options.IsCheckEnabled(pair.second.GetSanityLevel())) {
      if (!pair.second.MatchesOption(pair.first, this_ptr, that_ptr, options,
                                     mismatch) &&
          !pair.second.CheckByName(pair.first, this_ptr, that_ptr, options)) {
        return false;
      }
    }
  }
  return true;
}

bool OptionTypeInfo::CheckByName(const std::string& opt_name,
                                 const void* const this_ptr,
                                 const void* const that_ptr,
                                 const ConfigOptions& options) const {
  if (IsByName()) {
    std::string that_value;
    if (SerializeOption(opt_name, that_ptr, options, &that_value).ok()) {
      return CheckByName(opt_name, this_ptr, that_value, options);
    }
  }
  return false;
}

bool OptionTypeInfo::CheckByName(const std::string& opt_name,
                                 const void* const opt_ptr,
                                 const std::string& that_value,
                                 const ConfigOptions& options) const {
  std::string this_value;
  if (!IsByName()) {
    return false;
  } else if (!SerializeOption(opt_name, opt_ptr, options, &this_value).ok()) {
    return false;
  } else if (IsEnabled(OptionVerificationType::kByNameAllowFromNull)) {
    if (that_value == kNullptrString) {
      return true;
    }
  } else if (IsEnabled(OptionVerificationType::kByNameAllowNull)) {
    if (that_value == kNullptrString) {
      return true;
    }
  }
  return (this_value == that_value);
}

const OptionTypeInfo* OptionTypeInfo::FindOption(
    const std::string& opt_name,
    const std::unordered_map<std::string, OptionTypeInfo>& opt_map,
    std::string* elem_name) {
  const auto iter = opt_map.find(opt_name);  // Look up the value in the map
  if (iter != opt_map.end()) {               // Found the option in the map
    *elem_name = opt_name;                   // Return the name
    return &(iter->second);  // Return the contents of the iterator
  } else {
    auto idx = opt_name.find(".");              // Look for a separator
    if (idx > 0 && idx != std::string::npos) {  // We found a separator
      auto siter =
          opt_map.find(opt_name.substr(0, idx));  // Look for the short name
      if (siter != opt_map.end()) {               // We found the short name
        if (siter->second.IsStruct() ||           // If the object is a struct
            siter->second.IsConfigurable()) {     // or a Configurable
          *elem_name = opt_name.substr(idx + 1);  // Return the rest
          return &(siter->second);  // Return the contents of the iterator
        }
      }
    }
  }
  return nullptr;
}
#endif  // !ROCKSDB_LITE

}  // namespace ROCKSDB_NAMESPACE
