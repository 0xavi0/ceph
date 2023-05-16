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
#ifndef RGW_SFS_VERSION_TYPE_H
#define RGW_SFS_VERSION_TYPE_H

namespace rgw::sal::sfs {

enum class VersionType {
  REGULAR = 0,
  DELETE_MARKER,
  LAST_VALUE = DELETE_MARKER
};

}  // namespace rgw::sal::sfs

#endif  // RGW_SFS_VERSION_TYPE_H
