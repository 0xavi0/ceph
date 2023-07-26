#pragma once

#include <gtest/gtest.h>

#include "driver/sfs/sqlite/objects/object_definitions.h"
#include "include/uuid.h"
#include "rgw/driver/sfs/sqlite/versioned_object/versioned_object_definitions.h"
#include "rgw/rgw_sal_sfs.h"

template <typename T>
bool compare(const T& origin, const T& dest) {
  return origin == dest;
}

bool compare(const RGWAccessKey& origin, const RGWAccessKey& dest) {
  if (origin.id != dest.id) return false;
  if (origin.key != dest.key) return false;
  if (origin.subuser != dest.subuser) return false;
  return true;
}

bool compare(const RGWSubUser& origin, const RGWSubUser& dest) {
  if (origin.name != dest.name) return false;
  if (origin.perm_mask != dest.perm_mask) return false;
  return true;
}

template <typename T>
bool compareMaps(const T& origin, const T& dest) {
  if (origin.size() != dest.size()) return false;
  for (auto const& [key, val] : origin) {
    if (dest.find(key) == dest.end()) return false;
    auto& dest_val = dest.at(key);
    if (!compare(val, dest_val)) return false;
  }
  return true;
}

std::string getCapsString(const RGWUserCaps& caps) {
  auto formatter =
      std::make_unique<ceph::JSONFormatter>(new ceph::JSONFormatter(true));
  encode_json("caps", caps, formatter.get());
  std::ostringstream oss;
  formatter->flush(oss);
  return oss.str();
}

void compareUsersRGWInfo(const RGWUserInfo& origin, const RGWUserInfo& dest) {
  ASSERT_EQ(origin.user_id.id, dest.user_id.id);
  ASSERT_EQ(origin.user_id.tenant, dest.user_id.tenant);
  ASSERT_EQ(origin.user_id.ns, dest.user_id.ns);
  ASSERT_EQ(origin.display_name, dest.display_name);
  ASSERT_EQ(origin.user_email, dest.user_email);
  ASSERT_TRUE(compareMaps(origin.access_keys, dest.access_keys));
  ASSERT_TRUE(compareMaps(origin.swift_keys, dest.swift_keys));
  ASSERT_TRUE(compareMaps(origin.subusers, dest.subusers));
  ASSERT_EQ(origin.suspended, dest.suspended);
  ASSERT_EQ(origin.max_buckets, dest.max_buckets);
  ASSERT_EQ(origin.op_mask, dest.op_mask);
  ASSERT_EQ(getCapsString(origin.caps), getCapsString(dest.caps));
  ASSERT_EQ(origin.system, dest.system);
  ASSERT_EQ(origin.default_placement.name, dest.default_placement.name);
  ASSERT_EQ(
      origin.default_placement.storage_class,
      dest.default_placement.storage_class
  );
  ASSERT_EQ(origin.placement_tags, dest.placement_tags);
  ASSERT_EQ(
      origin.quota.bucket_quota.max_size, dest.quota.bucket_quota.max_size
  );
  ASSERT_EQ(
      origin.quota.bucket_quota.max_objects, dest.quota.bucket_quota.max_objects
  );
  ASSERT_EQ(origin.quota.bucket_quota.enabled, dest.quota.bucket_quota.enabled);
  ASSERT_EQ(
      origin.quota.bucket_quota.check_on_raw,
      dest.quota.bucket_quota.check_on_raw
  );
  ASSERT_TRUE(compareMaps(origin.temp_url_keys, dest.temp_url_keys));
  ASSERT_EQ(origin.quota.user_quota.max_size, dest.quota.user_quota.max_size);
  ASSERT_EQ(
      origin.quota.user_quota.max_objects, dest.quota.user_quota.max_objects
  );
  ASSERT_EQ(origin.quota.user_quota.enabled, dest.quota.user_quota.enabled);
  ASSERT_EQ(
      origin.quota.user_quota.check_on_raw, dest.quota.user_quota.check_on_raw
  );
  ASSERT_EQ(origin.type, dest.type);
  ASSERT_EQ(origin.mfa_ids, dest.mfa_ids);
  ASSERT_EQ(origin.assumed_role_arn, dest.assumed_role_arn);
}

void compareUserAttrs(
    const rgw::sal::Attrs& origin, const rgw::sal::Attrs& dest
) {
  ASSERT_TRUE(compareMaps(origin, dest));
}

void compareUserVersion(const obj_version& origin, const obj_version& dest) {
  ASSERT_EQ(origin.ver, dest.ver);
  ASSERT_EQ(origin.tag, dest.tag);
}

inline rgw::sal::sfs::sqlite::DBObject create_test_object(
    const std::string& bucket_id, const std::string& name
) {
  rgw::sal::sfs::sqlite::DBObject object;
  object.uuid.generate_random();
  object.bucket_id = bucket_id;
  object.name = name;
  return object;
}

inline rgw::sal::sfs::sqlite::DBVersionedObject create_test_versionedobject(
    const uuid_d& object_id, const std::string& version_id
) {
  rgw::sal::sfs::sqlite::DBVersionedObject test_versioned_object;
  test_versioned_object.object_id = object_id;
  test_versioned_object.size = 2342;
  test_versioned_object.create_time = ceph::real_clock::now();
  test_versioned_object.delete_time = ceph::real_clock::now();
  test_versioned_object.commit_time = ceph::real_clock::now();
  test_versioned_object.mtime = ceph::real_clock::now();
  test_versioned_object.object_state = rgw::sal::sfs::ObjectState::OPEN;
  test_versioned_object.version_id = version_id;
  test_versioned_object.etag = "test_etag_" + version_id;
  test_versioned_object.version_type = rgw::sal::sfs::VersionType::REGULAR;

  //set attrs with default ACL
  {
    RGWAccessControlPolicy aclp;
    rgw_user aclu("usertest");
    aclp.get_acl().create_default(aclu, "usertest");
    aclp.get_owner().set_name("usertest");
    aclp.get_owner().set_id(aclu);
    bufferlist acl_bl;
    aclp.encode(acl_bl);
    rgw::sal::Attrs attrs;
    attrs[RGW_ATTR_ACL] = acl_bl;
    test_versioned_object.attrs = attrs;
  }
  return test_versioned_object;
}
