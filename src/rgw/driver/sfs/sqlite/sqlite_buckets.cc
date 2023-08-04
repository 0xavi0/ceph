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
#include "sqlite_buckets.h"

#include <driver/sfs/sqlite/buckets/bucket_definitions.h>
#include <driver/sfs/sqlite/users/users_definitions.h>

using namespace sqlite_orm;

namespace rgw::sal::sfs::sqlite {

SQLiteBuckets::SQLiteBuckets(DBConnRef _conn) : conn(_conn) {}

std::vector<DBOPBucketInfo> get_rgw_buckets(
    const std::vector<DBBucket>& db_buckets
) {
  std::vector<DBOPBucketInfo> ret_buckets;
  for (const auto& db_bucket : db_buckets) {
    auto rgw_bucket = get_rgw_bucket(db_bucket);
    ret_buckets.push_back(rgw_bucket);
  }
  return ret_buckets;
}

std::optional<DBOPBucketInfo> SQLiteBuckets::get_bucket(
    const std::string& bucket_id
) const {
  auto storage = conn->get_storage();
  auto bucket = storage.get_pointer<DBBucket>(bucket_id);
  std::optional<DBOPBucketInfo> ret_value;
  if (bucket) {
    ret_value = get_rgw_bucket(*bucket);
  }
  return ret_value;
}

std::optional<std::pair<std::string, std::string>> SQLiteBuckets::get_owner(
    const std::string& bucket_id
) const {
  auto storage = conn->get_storage();
  const auto rows = storage.select(
      columns(&DBUser::user_id, &DBUser::display_name),
      inner_join<DBUser>(on(is_equal(&DBBucket::owner_id, &DBUser::user_id))),
      where(is_equal(&DBBucket::bucket_id, bucket_id))
  );
  if (rows.size() == 0) {
    return std::nullopt;
  }
  const auto row = rows[0];
  return std::make_pair(std::get<0>(row), std::get<1>(row).value_or(""));
}

std::vector<DBOPBucketInfo> SQLiteBuckets::get_bucket_by_name(
    const std::string& bucket_name
) const {
  auto storage = conn->get_storage();
  return get_rgw_buckets(
      storage.get_all<DBBucket>(where(c(&DBBucket::bucket_name) = bucket_name))
  );
}

void SQLiteBuckets::store_bucket(const DBOPBucketInfo& bucket) const {
  auto storage = conn->get_storage();
  auto db_bucket = get_db_bucket(bucket);
  storage.replace(db_bucket);
}

void SQLiteBuckets::remove_bucket(const std::string& bucket_name) const {
  auto storage = conn->get_storage();
  storage.remove<DBBucket>(bucket_name);
}

std::vector<std::string> SQLiteBuckets::get_bucket_ids() const {
  auto storage = conn->get_storage();
  return storage.select(&DBBucket::bucket_name);
}

std::vector<std::string> SQLiteBuckets::get_bucket_ids(
    const std::string& user_id
) const {
  auto storage = conn->get_storage();
  return storage.select(
      &DBBucket::bucket_name, where(c(&DBBucket::owner_id) = user_id)
  );
}

std::vector<DBOPBucketInfo> SQLiteBuckets::get_buckets() const {
  auto storage = conn->get_storage();
  return get_rgw_buckets(storage.get_all<DBBucket>());
}

std::vector<DBOPBucketInfo> SQLiteBuckets::get_buckets(
    const std::string& user_id
) const {
  auto storage = conn->get_storage();
  return get_rgw_buckets(
      storage.get_all<DBBucket>(where(c(&DBBucket::owner_id) = user_id))
  );
}

std::vector<std::string> SQLiteBuckets::get_deleted_buckets_ids() const {
  auto storage = conn->get_storage();
  return storage.select(
      &DBBucket::bucket_id, where(c(&DBBucket::deleted) = true)
  );
}

bool SQLiteBuckets::bucket_empty(const std::string& bucket_id) const {
  auto storage = conn->get_storage();
  auto num_ids = storage.count<DBVersionedObject>(
      inner_join<DBObject>(
          on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
      ),
      where(
          is_equal(&DBVersionedObject::object_state, ObjectState::COMMITTED) and
          is_equal(&DBObject::bucket_id, bucket_id) and
          is_equal(&DBVersionedObject::version_type, VersionType::REGULAR)
      )
  );
  return num_ids == 0;
}

}  // namespace rgw::sal::sfs::sqlite
