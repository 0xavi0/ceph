// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
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
#include "lifecycle/lifecycle_definitions.h"

namespace rgw::sal::sfs::sqlite {

class SQLiteLifecycle {
  DBConnRef conn;

 public:
  explicit SQLiteLifecycle(DBConnRef _conn);
  virtual ~SQLiteLifecycle() = default;

  SQLiteLifecycle(const SQLiteLifecycle&) = delete;
  SQLiteLifecycle& operator=(const SQLiteLifecycle&) = delete;

  DBOPLCHead get_head(const std::string& oid) const;
  void store_head(const DBOPLCHead& head) const;
  void remove_head(const std::string& oid) const;

  std::optional<DBOPLCEntry> get_entry(
      const std::string& oid, const std::string& marker
  ) const;
  std::optional<DBOPLCEntry> get_next_entry(
      const std::string& oid, const std::string& marker
  ) const;
  void store_entry(const DBOPLCEntry& entry) const;
  void remove_entry(const std::string& oid, const std::string& marker) const;

  std::vector<DBOPLCEntry> list_entries(
      const std::string& oid, const std::string& marker, uint32_t max_entries
  ) const;
};

}  // namespace rgw::sal::sfs::sqlite
