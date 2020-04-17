// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/db_plugin.h"

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "options/customizable_helper.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

Status DBPlugin::CreateFromString(const std::string& value,
                                  const ConfigOptions& opts,
                                  std::shared_ptr<DBPlugin>* result) {
  return LoadSharedObject<DBPlugin>(value, nullptr, opts, result);
}

Status DBPlugin::NotSupported(OpenMode /*mode*/) const {
  return Status::NotSupported("Open mode not supported ", Name());
}

const DBPlugin* DBPlugin::Find(const std::string& id,
                               const DBOptions& db_opts) {
  return Find(id, db_opts.plugins);
}

const DBPlugin* DBPlugin::Find(
    const std::string& id,
    const std::vector<std::shared_ptr<DBPlugin>>& plugins) {
  for (const auto p : plugins) {
    if (p->FindInstance(id) != nullptr) {
      return p.get();
    }
  }
  return nullptr;
}

Status DBPlugin::SanitizeOptions(
    OpenMode open_mode, const std::string& db_name, DBOptions* db_options,
    std::vector<ColumnFamilyDescriptor>* column_families) {
  Status s;
  for (auto p : db_options->plugins) {
    if (p->SupportsOpenMode(open_mode)) {
      s = p->SanitizeCB(open_mode, db_name, db_options, column_families);
      if (!s.ok()) {
        return s;
      }
    } else {
      return p->NotSupported(open_mode);
    }
  }
  return Status::OK();
}
static Status ValidateOptionsByTable(
    const DBOptions& db_opts,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (auto cf : column_families) {
    s = ValidateOptions(db_opts, cf.options);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

// Validate self-consistency of DB options and its consistency with cf options
Status DBPlugin::ValidateOptions(
    OpenMode open_mode, const std::string& db_name, const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s = ValidateOptionsByTable(db_options, column_families);
  if (!s.ok()) {
    return s;
  }
  for (auto p : db_options.plugins) {
    if (p->SupportsOpenMode(open_mode)) {
      s = p->ValidateCB(open_mode, db_name, db_options, column_families);
      if (!s.ok()) {
        return s;
      }
    } else {
      return p->NotSupported(open_mode);
    }
  }
  for (auto& cfd : column_families) {
    s = ColumnFamilyData::ValidateOptions(db_options, cfd.options);
    if (!s.ok()) {
      return s;
    }
  }
  return DBImpl::ValidateOptions(db_options);
}

Status DBPlugin::OpenCB(OpenMode mode, DB* db,
                        const std::vector<ColumnFamilyHandle*>& /*handles*/,
                        DB** wrapped) {
  assert(SupportsOpenMode(mode));
  *wrapped = db;
  return Status::OK();
}

Status DBPlugin::Open(OpenMode open_mode, DB* db,
                      const std::vector<ColumnFamilyHandle*>& handles,
                      DB** wrapped) {
  Status s;

  *wrapped = db;
  DBOptions db_opts = db->GetDBOptions();
  for (const auto p : db_opts.plugins) {
    if (p->SupportsOpenMode(open_mode)) {
      s = p->OpenCB(open_mode, *wrapped, handles, wrapped);
      if (!s.ok()) {
        return s;
      }
    } else {
      return p->NotSupported(open_mode);
    }
  }
  return Status::OK();
}

Status DBPlugin::RepairDB(
    const std::string& dbname, const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const ColumnFamilyOptions& unknown_cf_opts) {
  Status s;
  for (const auto p : db_options.plugins) {
    s = p->RepairCB(dbname, db_options, column_families, unknown_cf_opts);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status DBPlugin::DestroyDB(
    const std::string& dbname, const Options& options,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (const auto p : options.plugins) {
    s = p->DestroyCB(dbname, options, column_families);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
