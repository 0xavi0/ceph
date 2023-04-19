// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#pragma once

#include <memory>

#include "rgw_sal.h"
#include "rgw_sal_sfs.h"

#define sfs_dout_subsys ceph_subsys_rgw

namespace rgw::sal::sfs {

class SFSGC : public DoutPrefixProvider {
  CephContext* cct = nullptr;
  SFStore* store = nullptr;
  std::atomic<bool> down_flag = {true};
  std::atomic<bool> suspend_flag = {false};
  long int max_objects;
  std::unique_ptr<sfs::ObjectDeleter> deleter;

  class GCWorker : public Thread {
    const DoutPrefixProvider* dpp = nullptr;
    CephContext* cct = nullptr;
    SFSGC* gc = nullptr;
    ceph::mutex lock = ceph::make_mutex("GCWorker");
    ceph::condition_variable cond;

    std::string get_cls_name() const { return "GCWorker"; }

   public:
    GCWorker(const DoutPrefixProvider* _dpp, CephContext* _cct, SFSGC* _gc);

    void* entry() override;
    void stop();
  };

  std::unique_ptr<GCWorker> worker = nullptr;

 public:
  SFSGC(CephContext*, SFStore*);
  ~SFSGC();

  int process();

  bool going_down();
  void initialize();
  bool suspended();
  void suspend();
  void resume();

  CephContext* get_cct() const override { return store->ctx(); }
  unsigned get_subsys() const override { return sfs_dout_subsys; }

  std::ostream& gen_prefix(std::ostream& out) const override;

  std::string get_cls_name() const { return "SFSGC"; }

 private:
  int process_deleted_buckets();
  int process_deleted_versions();

  int delete_objects(const std::string& bucket_id);
  int delete_bucket(const std::string& bucket_id);

  int delete_object(const uuid_d& uuid, bool& whole_object_deleted);
  int delete_version(const uuid_d& uuid, uint version);

  void log_gc_error(int error, const uuid_d& uuid) const;
  void log_gc_error(int error, const uuid_d& uuid, uint version) const;

  bool able_to_continue_after_error(int error) const;
};

}  //  namespace rgw::sal::sfs
