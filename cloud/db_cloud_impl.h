// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"

namespace rocksdb {

//
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class DBCloudImpl : public DBCloud {
  friend DBCloud;
  friend class DBCloudPlugin;

 public:
  explicit DBCloudImpl(DB* db);
  virtual ~DBCloudImpl();
  Status Savepoint() override;

  Status CheckpointToCloud(const BucketOptions& destination,
                           const CheckpointToCloudOptions& options) override;

  Status ExecuteRemoteCompactionRequest(
      const PluggableCompactionParam& inputParams,
      PluggableCompactionResult* result,
      bool sanitize) override;

 protected:
  // The CloudEnv used by this open instance.
  CloudEnv* cenv_;

 private:
  Status DoCheckpointToCloud(const BucketOptions& destination,
                             const CheckpointToCloudOptions& options);

};
}
#endif  // ROCKSDB_LITE
