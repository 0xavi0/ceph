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

#include <chrono>
#include <mutex>

#include "rgw_lc.h"
#include "rgw_sal.h"
#include "rgw_sal_sfs.h"

namespace rgw::sal::sfs {

class LCSFSSerializer : public StoreLCSerializer {
  std::timed_mutex& mutex;

 public:
  LCSFSSerializer(
      std::timed_mutex& m, SFStore* /* store */, const std::string& /* oid */,
      const std::string& /* lock_name */, const std::string& /* cookie */
  )
      : mutex(m) {}

  virtual int try_lock(
      const DoutPrefixProvider* /* dpp */, utime_t dur, optional_yield /* y */
  ) override {
    if (mutex.try_lock_for(std::chrono::seconds(dur))) {
      return 0;
    }
    return -EBUSY;
  }
  virtual int unlock() override {
    mutex.unlock();
    return 0;
  }
};

class SFSLifecycle : public StoreLifecycle {
  SFStore* store;
  std::map<std::string, std::timed_mutex> mutex_map;

 public:
  SFSLifecycle(SFStore* _st);

  using StoreLifecycle::get_entry;
  virtual int get_entry(
      const std::string& oid, const std::string& marker,
      std::unique_ptr<LCEntry>* entry
  ) override;
  virtual int get_next_entry(
      const std::string& oid, const std::string& marker,
      std::unique_ptr<LCEntry>* entry
  ) override;
  virtual int set_entry(const std::string& oid, LCEntry& entry) override;
  virtual int list_entries(
      const std::string& oid, const std::string& marker, uint32_t max_entries,
      std::vector<std::unique_ptr<LCEntry>>& entries
  ) override;
  virtual int rm_entry(const std::string& oid, LCEntry& entry) override;
  virtual int get_head(const std::string& oid, std::unique_ptr<LCHead>* head)
      override;
  virtual int put_head(const std::string& oid, LCHead& head) override;
  virtual std::unique_ptr<LCSerializer> get_serializer(
      const std::string& lock_name, const std::string& oid,
      const std::string& cookie
  ) override;
};

}  // namespace rgw::sal::sfs
