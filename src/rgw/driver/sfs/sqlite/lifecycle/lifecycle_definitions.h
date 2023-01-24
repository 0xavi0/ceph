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

#include <string>

#include "rgw_common.h"

namespace rgw::sal::sfs::sqlite {

struct DBOPLCHead {
  std::string lc_index;  // primary key
  std::string marker;
  long start_date;
};

struct DBOPLCEntry {
  std::string lc_index;     // composite primary key
  std::string bucket_name;  // composite primary key
  uint64_t start_time;
  uint32_t status;
};

}  // namespace rgw::sal::sfs::sqlite
