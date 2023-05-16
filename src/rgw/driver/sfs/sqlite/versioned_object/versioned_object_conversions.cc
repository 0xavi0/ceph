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
#include "versioned_object_conversions.h"

#include "../conversion_utils.h"

namespace rgw::sal::sfs::sqlite {

ObjectState get_object_state(uint state) {
  if (state > static_cast<uint>(ObjectState::LAST_VALUE)) {
    throw(std::runtime_error(
        "incorrect state found (" + std::to_string(state) + ")"
    ));
  }
  return static_cast<ObjectState>(state);
}

uint get_uint_object_state(ObjectState state) {
  return static_cast<uint>(state);
}

DBOPVersionedObjectInfo get_rgw_versioned_object(const DBVersionedObject& object
) {
  DBOPVersionedObjectInfo rgw_object;
  rgw_object.id = object.id;
  rgw_object.object_id = object.object_id;
  rgw_object.checksum = object.checksum;
  rgw_object.size = object.size;
  rgw_object.create_time = object.create_time;
  rgw_object.delete_time = object.delete_time;
  rgw_object.commit_time = object.commit_time;
  rgw_object.mtime = object.mtime;
  rgw_object.object_state = object.object_state;
  rgw_object.version_id = object.version_id;
  rgw_object.etag = object.etag;
  assign_optional_value(object.attrs, rgw_object.attrs);
  rgw_object.version_type = object.version_type;
  return rgw_object;
}

DBVersionedObject get_db_versioned_object(const DBOPVersionedObjectInfo& object
) {
  DBVersionedObject db_object;
  db_object.id = object.id;
  db_object.object_id = object.object_id;
  db_object.checksum = object.checksum;
  db_object.size = object.size;
  db_object.create_time = object.create_time;
  db_object.delete_time = object.delete_time;
  db_object.commit_time = object.commit_time;
  db_object.mtime = object.mtime;
  db_object.object_state = object.object_state;
  db_object.version_id = object.version_id;
  db_object.etag = object.etag;
  assign_db_value(object.attrs, db_object.attrs);
  db_object.version_type = object.version_type;
  return db_object;
}
}  // namespace rgw::sal::sfs::sqlite
