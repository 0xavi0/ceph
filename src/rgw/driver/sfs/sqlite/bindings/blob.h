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

#include <type_traits>

#include "rgw/driver/sfs/sqlite/conversion_utils.h"
#include "rgw/driver/sfs/sqlite/sqlite_orm.h"
#include "rgw_common.h"

namespace sqlite_orm {

template <typename T>
struct __is_sqlite_blob : std::false_type {};

template <typename T>
inline constexpr bool is_sqlite_blob = __is_sqlite_blob<T>::value;

template <>
struct __is_sqlite_blob<rgw::sal::Attrs> : std::true_type {};

template <>
struct __is_sqlite_blob<ACLOwner> : std::true_type {};

template <class T>
struct type_printer<T, typename std::enable_if<is_sqlite_blob<T>, void>::type>
    : public blob_printer {};

template <class T>
struct statement_binder<
    T, typename std::enable_if<is_sqlite_blob<T>, void>::type> {
  int bind(sqlite3_stmt* stmt, int index, const T& value) {
    std::vector<char> blobValue;
    rgw::sal::sfs::sqlite::encode_blob(value, blobValue);
    return statement_binder<std::vector<char>>().bind(stmt, index, blobValue);
  }
};

template <class T>
struct field_printer<
    T, typename std::enable_if<is_sqlite_blob<T>, void>::type> {
  std::string operator()(const T& value) const { return "ENCODED BLOB"; }
};

template <class T>
struct row_extractor<
    T, typename std::enable_if<is_sqlite_blob<T>, void>::type> {
  T extract(sqlite3_stmt* stmt, int columnIndex) {
    auto blob_data = sqlite3_column_blob(stmt, columnIndex);
    auto blob_size = sqlite3_column_bytes(stmt, columnIndex);
    if (blob_data == nullptr || blob_size < 0) {
      throw(std::system_error(
          ERANGE, std::system_category(),
          "Invalid blob at column : (" + std::to_string(columnIndex) + ")"
      ));
    }
    T ret;
    rgw::sal::sfs::sqlite::decode_blob(
        reinterpret_cast<const char*>(blob_data),
        static_cast<size_t>(blob_size), ret
    );
    return ret;
  }
};
}  // namespace sqlite_orm
