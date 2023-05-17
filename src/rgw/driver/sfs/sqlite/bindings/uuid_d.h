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
#include "rgw_common.h"

namespace sqlite_orm {
template <>
struct type_printer<uuid_d> : public text_printer {};

template <>
struct statement_binder<uuid_d> {
  int bind(sqlite3_stmt* stmt, int index, const uuid_d& value) const {
    return statement_binder<std::string>().bind(stmt, index, value.to_string());
  }
};

template <>
struct field_printer<uuid_d> {
  std::string operator()(const uuid_d& value) const {
    return value.to_string();
  }
};

template <>
struct row_extractor<uuid_d> {
  uuid_d extract(const char* row_value) const {
    if (row_value) {
      uuid_d ret_value;
      if (!ret_value.parse(row_value)) {
        throw std::system_error(
            ERANGE, std::system_category(),
            "incorrect uuid string (" + std::string(row_value) + ")"
        );
      }
      return ret_value;
    } else {
      // ! row_value
      throw std::system_error(
          ERANGE, std::system_category(), "incorrect uuid string (nullptr)"
      );
    }
  }

  uuid_d extract(sqlite3_stmt* stmt, int columnIndex) const {
    auto str = sqlite3_column_text(stmt, columnIndex);
    // sqlite3_colume_text returns const unsigned char*
    return this->extract(reinterpret_cast<const char*>(str));
  }
  uuid_d extract(sqlite3_value* row_value) const {
    // sqlite3_colume_text returns const unsigned char*
    auto characters =
        reinterpret_cast<const char*>(sqlite3_value_text(row_value));
    return extract(characters);
  }
};
}  // namespace sqlite_orm
