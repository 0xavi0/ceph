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
#include "sfs_gc.h"

#include "driver/sfs/types.h"
#include "rgw/driver/sfs/sfs_errors.h"
#include "rgw/driver/sfs/sqlite/sqlite_objects.h"

namespace rgw::sal::sfs {

class BucketMetadataError : std::exception {
  std::string _bucket_name;

 public:
  BucketMetadataError(const std::string& bucket_name)
      : _bucket_name(bucket_name) {}

  const std::string& bucket_name() const noexcept { return _bucket_name; }

  const char* what() const noexcept {
    std::ostringstream oss;
    oss << "Error deleting bucket metadata for bucket: " << _bucket_name;
    return oss.str().c_str();
  }
};

bool can_delete_whole_object(size_t nb_versions, long int max_objects) {
  auto items_in_object = static_cast<long int>(nb_versions + 1);
  return (max_objects - items_in_object) >= 0;
}

SFSGC::SFSGC(CephContext* _cctx, SFStore* _store)
    : cct(_cctx),
      store(_store),
      deleter(new sfs::ObjectDeleter(store->db_conn, store->get_data_path())) {
  worker = std::make_unique<GCWorker>(this, cct, this);
}

SFSGC::~SFSGC() {
  down_flag = true;
  if (worker->is_started()) {
    worker->stop();
    worker->join();
  }
}

int SFSGC::process() {
  // This is the method that does the garbage collection.

  // set the maximum number of objects we can delete in this iteration
  max_objects = cct->_conf->rgw_gc_max_objs;
  lsfs_dout(this, 10) << "garbage collection: processing with max_objects = "
                      << max_objects << dendl;

  auto ret = process_deleted_buckets();
  if (ret < 0) {
    return ret;
  }
  ret = process_deleted_versions();
  if (ret < 0) {
    return ret;
  }

  return 0;
}

bool SFSGC::going_down() {
  return down_flag;
}

/*
 * The constructor must have finished before the worker thread can be created,
 * because otherwise the logging will dereference an invalid pointer, since the
 * SFSGC instance is a prefix provider for the logging in the worker thread
 */
void SFSGC::initialize() {
  worker->create("rgw_gc");
  down_flag = false;
}

bool SFSGC::suspended() {
  return suspend_flag;
}

void SFSGC::suspend() {
  suspend_flag = true;
}

void SFSGC::resume() {
  suspend_flag = false;
}

std::ostream& SFSGC::gen_prefix(std::ostream& out) const {
  return out << "garbage collection: ";
}

int SFSGC::process_deleted_buckets() {
  // permanently delete removed buckets and their objects and versions
  sqlite::SQLiteBuckets db_buckets(store->db_conn);
  auto deleted_buckets = db_buckets.get_deleted_buckets_ids();
  lsfs_dout(this, 10) << "deleted buckets found = " << deleted_buckets.size()
                      << dendl;
  for (auto const& bucket_id : deleted_buckets) {
    if (max_objects <= 0) {
      break;
    }
    auto ret = delete_bucket(bucket_id);
    if (ret < 0) {
      if (!able_to_continue_after_error(ret)) {
        return ret;
      }
    }
  }
  return 0;
}

int SFSGC::process_deleted_versions() {
  sqlite::SQLiteVersionedObjects db_versions(store->db_conn);
  std::map<uuid_d, bool> already_deleted;

  // get deleted versions ordered by descending size
  auto versions =
      db_versions.get_deleted_versioned_objects_highest_priority_first(
          max_objects
      );
  for (auto const& version : versions) {
    if (max_objects <= 0) {
      break;
    }
    if (already_deleted.contains(version.object_id)) {
      // skip if the whole object was already deleted
      continue;
    }
    lsfs_dout(this, 30) << "Checking deleted version: [" << version.object_id
                        << ", " << version.id << "]" << dendl;
    // check if object has been deleted or it's just a noncurrent version
    auto last_version =
        db_versions.get_last_versioned_object(version.object_id);
    if (last_version->object_state == ObjectState::DELETED) {
      // object is deleted... try to delete the object and its versions
      bool whole_object_deleted = false;
      auto ret = delete_object(version.object_id, whole_object_deleted);
      if (ret < 0) {
        if (!able_to_continue_after_error(ret)) {
          return ret;
        }
      } else {
        if (whole_object_deleted) {
          already_deleted[version.object_id] = true;
          lsfs_dout(this, 30)
              << "Deleted object: " << version.object_id << dendl;
        }
      }
    } else {
      // this is a noncurrent version
      auto ret = delete_version(version.object_id, version.id);
      if (ret < 0) {
        if (!able_to_continue_after_error(ret)) {
          return ret;
        }
      } else {
        lsfs_dout(this, 30) << "Deleted version: [" << version.object_id << ", "
                            << version.id << "]" << dendl;
      }
    }
  }
  return 0;
}

int SFSGC::delete_objects(const std::string& bucket_id) {
  sqlite::SQLiteObjects db_objs(store->db_conn);
  auto object_ids = db_objs.get_object_ids(bucket_id);
  int last_error_deleting_objects = 0;
  for (auto const& id : object_ids) {
    if (max_objects <= 0) {
      break;
    }
    bool whole_object_deleted;
    auto ret = delete_object(id, whole_object_deleted);
    if (ret < 0) {
      // as we might continue trying to delete objects, we keep the last
      // possible error.
      // In case that any error occurred when deleting any object, the bucket
      // cannot be deleted. (or it will throw a foreign key violation exception)
      last_error_deleting_objects = ret;
      if (!able_to_continue_after_error(ret)) {
        return ret;
      }
    }
  }
  return last_error_deleting_objects;
}

int SFSGC::delete_bucket(const std::string& bucket_id) {
  // delete the objects of the bucket first
  auto ret = delete_objects(bucket_id);
  if (ret < 0) {
    return ret;
  }
  if (max_objects > 0) {
    try {
      sqlite::SQLiteBuckets db_buckets(store->db_conn);
      db_buckets.remove_bucket(bucket_id);
      lsfs_dout(this, 30) << "Deleted bucket: " << bucket_id << dendl;
      --max_objects;
    } catch (const std::system_error& e) {
      return -ERR_SFS_METADATA_DELETE_ERROR;
    }
  }
  return 0;
}

int SFSGC::delete_object(const uuid_d& uuid, bool& whole_object_deleted) {
  // can we delete the whole object?
  sqlite::SQLiteVersionedObjects db_versions(store->db_conn);
  auto versions = db_versions.get_versioned_object_ids(uuid);
  if (can_delete_whole_object(versions.size(), max_objects)) {
    // can delete the whole object
    auto ret = deleter->delete_object(uuid);
    if (ret < 0) {
      log_gc_error(ret, uuid);
      return ret;
    }
    max_objects -= (versions.size() + 1);
    whole_object_deleted = true;
  } else {
    for (auto const& version : versions) {
      if (max_objects <= 0) {
        break;
      }
      auto ret = delete_version(uuid, version);
      if (ret < 0) {
        return ret;
      }
      --max_objects;
      lsfs_dout(this, 30) << "Deleted version: [" << uuid << ", " << version
                          << "]" << dendl;
    }
    if (max_objects > 0) {
      std::vector<uint> versions;
      auto ret = deleter->delete_object_metadata(uuid, versions);
      if (ret < 0) {
        log_gc_error(ret, uuid);
        return ret;
      }
      whole_object_deleted = true;
      --max_objects;
    }
  }
  return 0;
}

int SFSGC::delete_version(const uuid_d& uuid, uint version) {
  auto ret = deleter->delete_version(uuid, version);
  if (ret < 0) {
    log_gc_error(ret, uuid, version);
  }
  return ret;
}

void SFSGC::log_gc_error(int error, const uuid_d& uuid) const {
  if (error == -ERR_SFS_METADATA_DELETE_ERROR) {
    lsfs_dout(this, 30) << "Error deleting metadata for object: " << uuid
                        << ". Retrying next iteration." << dendl;
  } else if (error == -EIO) {
    lsfs_dout(this, 30) << "Error deleting data for object: " << uuid
                        << ". Orphan files may exist." << dendl;
  } else {
    lsfs_dout(this, 30) << "Unknown error deleting object: " << uuid
                        << ". Orphan files may exist." << dendl;
  }
}

void SFSGC::log_gc_error(int error, const uuid_d& uuid, uint version) const {
  if (error == -ERR_SFS_METADATA_DELETE_ERROR) {
    lsfs_dout(this, 30) << "Error deleting metadata for version: [" << uuid
                        << "," << version << "]"
                        << ". Retrying next iteration." << dendl;
  } else if (error == -EIO) {
    lsfs_dout(this, 30) << "Error deleting for version : [" << uuid << ","
                        << version << "]"
                        << ". Orphan files may exist." << dendl;
  } else {
    lsfs_dout(this, 30) << "Unknown error deleting version: [" << uuid << ","
                        << version << "]"
                        << ". Orphan files may exist." << dendl;
  }
}

bool SFSGC::able_to_continue_after_error(int error) const {
  return (error == -ERR_SFS_METADATA_DELETE_ERROR || error == -EIO);
}

SFSGC::GCWorker::GCWorker(
    const DoutPrefixProvider* _dpp, CephContext* _cct, SFSGC* _gc
)
    : dpp(_dpp), cct(_cct), gc(_gc) {}

void* SFSGC::GCWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    lsfs_dout(dpp, 2) << "start" << dendl;

    if (!gc->suspended()) {
      int r = gc->process();
      if (r < 0) {
        lsfs_dout(
            dpp, 0
        ) << "ERROR: garbage collection process() returned error r="
          << r << dendl;
      }
      lsfs_dout(dpp, 2) << "stop" << dendl;
    }

    if (gc->going_down()) break;

    utime_t end = ceph_clock_now();
    end -= start;
    int secs = cct->_conf->rgw_gc_processor_period;
    secs -= end.sec();
    if (secs <= 0) {
      // in case the GC iteration took more time than the period
      secs = cct->_conf->rgw_gc_processor_period;
    }

    std::unique_lock locker{lock};
    cond.wait_for(locker, std::chrono::seconds(secs));
  } while (!gc->going_down());

  return nullptr;
}

void SFSGC::GCWorker::stop() {
  std::lock_guard l{lock};
  cond.notify_all();
}

}  //  namespace rgw::sal::sfs
