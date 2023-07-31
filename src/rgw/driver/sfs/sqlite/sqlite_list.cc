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
#include "sqlite_list.h"

#include <limits>

#include "driver/sfs/version_type.h"
#include "objects/object_definitions.h"
#include "rgw_obj_types.h"
#include "sqlite_orm.h"
#include "versioned_object/versioned_object_definitions.h"

using namespace sqlite_orm;
namespace rgw::sal::sfs::sqlite {

SQLiteList::SQLiteList(DBConnRef _conn) : conn(_conn) {}

static std::string prefix_to_like_expr(const std::string& prefix) {
  std::string result(prefix);
  // TODO(https://github.com/aquarist-labs/s3gw/issues/643) implement me
  result.append("%");
  return result;
}

bool SQLiteList::objects(
    const std::string& bucket_id, const std::string& prefix,
    const std::string& start_after_object_name, size_t max,
    std::vector<rgw_bucket_dir_entry>& out, bool* out_more_available
) const {
  ceph_assert(!bucket_id.empty());

  // more available logic: request one more than max. if we get that
  // much set out_more_available, but return only up to max
  ceph_assert(max < std::numeric_limits<size_t>::max());
  const size_t query_limit = max + 1;

  // ListBucket does not care about versions/instances. don't populate
  // key.instance
  auto storage = conn->get_storage();
  auto rows = storage.select(
      columns(
          &DBObject::name, &DBVersionedObject::mtime, &DBVersionedObject::etag,
          sum(&DBVersionedObject::size)
      ),
      inner_join<DBVersionedObject>(
          on(is_equal(&DBObject::uuid, &DBVersionedObject::object_id))
      ),
      where(
          is_equal(&DBVersionedObject::object_state, ObjectState::COMMITTED) and
          is_equal(&DBObject::bucket_id, bucket_id) and
          greater_than(&DBObject::name, start_after_object_name) and
          like(&DBObject::name, prefix_to_like_expr(prefix))
      ),
      group_by(&DBObject::name),
      having(is_equal(
          sqlite_orm::max(&DBVersionedObject::version_type),
          VersionType::REGULAR
      )),
      order_by(&DBObject::name), limit(query_limit)
  );
  ceph_assert(rows.size() <= static_cast<size_t>(query_limit));
  const size_t return_limit = std::min(max, rows.size());
  out.reserve(return_limit);
  for (size_t i = 0; i < return_limit; i++) {
    const auto& row = rows[i];
    rgw_bucket_dir_entry e;
    e.key.name = std::get<0>(row);
    e.meta.mtime = std::get<1>(row);
    e.meta.etag = std::get<2>(row);
    e.meta.size = static_cast<uint64_t>(*std::get<3>(row));
    e.meta.accounted_size = e.meta.size;
    // TODO(https://github.com/aquarist-labs/s3gw/issues/644) set owner
    out.emplace_back(e);
  }
  if (out_more_available) {
    *out_more_available = rows.size() == query_limit;
  }
  return true;
}

void SQLiteList::roll_up_common_prefixes(
    const std::string& find_after_prefix, const std::string& delimiter,
    const std::vector<rgw_bucket_dir_entry>& objects,
    std::map<std::string, bool>& out_common_prefixes,
    std::vector<rgw_bucket_dir_entry>& out_objects
) const {
  const size_t find_after_pos = find_after_prefix.length();
  const size_t delim_len = delimiter.length();
  if (delimiter.empty()) {
    out_objects = objects;
    return;
  }
  const std::string* prefix{nullptr};  // Last added prefix
  for (size_t i = 0; i < objects.size(); i++) {
    const std::string& name = objects[i].key.name;
    // Same prefix -> skip
    if (prefix != nullptr && name.starts_with(*prefix)) {
      continue;
    }
    if (name.starts_with(find_after_prefix)) {
      // Found delim -> add, remember prefix
      auto delim_pos = name.find(delimiter, find_after_pos);
      if (delim_pos != name.npos) {
        prefix = &out_common_prefixes
                      .emplace(name.substr(0, delim_pos + delim_len), true)
                      .first->first;
        continue;
      }
    }
    // Not found -> next
    out_objects.push_back(objects[i]);
  }
}

}  // namespace rgw::sal::sfs::sqlite
