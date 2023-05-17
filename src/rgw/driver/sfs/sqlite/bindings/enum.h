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

#include "rgw/driver/sfs/sqlite/sqlite_orm.h"

/// sqliteorm binding for enum types.
/// This binding can be used for any enum that has the LAST_VALUE value set to
/// the last possible value and the initial value set to 0.
/// This is used for the conversion from uint to enum to ensure that the uint
/// value is not out of range.
namespace sqlite_orm {
template <class T>
struct type_printer<T, typename std::enable_if<std::is_enum_v<T>>::type>
    : public integer_printer {};

template <class T>
struct statement_binder<T, typename std::enable_if<std::is_enum_v<T>>::type> {
  int bind(sqlite3_stmt* stmt, int index, const T& value) const {
    return statement_binder<uint>().bind(stmt, index, static_cast<uint>(value));
  }
};

template <class T>
struct field_printer<T, typename std::enable_if<std::is_enum_v<T>>::type> {
  std::string operator()(const T& value) const {
    return std::to_string(static_cast<uint>(value));
  }
};

template <class T>
struct row_extractor<T, typename std::enable_if<std::is_enum_v<T>>::type> {
  T extract(uint row_value) const {
    if (row_value > static_cast<uint>(T::LAST_VALUE)) {
      throw(std::system_error(
          ERANGE, std::system_category(),
          "Invalid enum value found: (" + std::to_string(row_value) + ")"
      ));
    }
    return static_cast<T>(row_value);
  }

  T extract(sqlite3_stmt* stmt, int columnIndex) const {
    auto int_value = sqlite3_column_int(stmt, columnIndex);
    if (int_value < 0) {
      throw(std::system_error(
          ERANGE, std::system_category(),
          "Invalid enum value found: (" + std::to_string(int_value) + ")"
      ));
    }
    return this->extract(static_cast<uint>(int_value));
  }
};

}  // namespace sqlite_orm
