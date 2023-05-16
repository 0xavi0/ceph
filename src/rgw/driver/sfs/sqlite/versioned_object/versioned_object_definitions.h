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

#include <string>

#include "rgw/driver/sfs/object_state.h"
#include "rgw/driver/sfs/sqlite/bindings/enum.h"
#include "rgw/driver/sfs/sqlite/bindings/real_time.h"
#include "rgw/driver/sfs/version_type.h"
#include "rgw/rgw_common.h"
#include "rgw_common.h"

namespace rgw::sal::sfs::sqlite {

using BLOB = std::vector<char>;

struct DBVersionedObject {
  uint id;
  uuid_d object_id;
  std::string checksum;
  size_t size;
  ceph::real_time create_time;
  ceph::real_time delete_time;
  ceph::real_time commit_time;
  ceph::real_time mtime;
  ObjectState object_state;
  std::string version_id;
  std::string etag;
  std::optional<BLOB> attrs;
  VersionType version_type = rgw::sal::sfs::VersionType::REGULAR;
};

struct DBOPVersionedObjectInfo {
  uint id;
  uuid_d object_id;
  std::string checksum;
  size_t size;
  ceph::real_time create_time;
  ceph::real_time delete_time;
  ceph::real_time commit_time;
  ceph::real_time mtime;
  ObjectState object_state;
  std::string version_id;
  std::string etag;
  rgw::sal::Attrs attrs;
  VersionType version_type = rgw::sal::sfs::VersionType::REGULAR;
};

}  // namespace rgw::sal::sfs::sqlite
