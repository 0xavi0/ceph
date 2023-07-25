/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2023 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#pragma once

#include "dbconn.h"
#include "rgw_sal.h"

namespace rgw::sal::sfs::sqlite {

class SQLiteList {
  DBConnRef conn;

 public:
  explicit SQLiteList(DBConnRef _conn);
  virtual ~SQLiteList() = default;

  /// objects lists committed objects in bucket, with optional prefix
  /// search and pagination (max, start_after_object_name). Optionally
  /// sets out_more_available to indicate callers to call again with
  /// pagination.
  bool objects(
      const std::string& bucket_id, const std::string& prefix,
      const std::string& start_after_object_name, size_t max,
      std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available = nullptr
  ) const;
};
}  // namespace rgw::sal::sfs::sqlite
