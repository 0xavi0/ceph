// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

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
 *
 */

#include "rgw_status_page_telemetry.h"

#include <fmt/chrono.h>

#include <chrono>

#include "common/ceph_json.h"
#include "common/version.h"
#include "rgw_s3gw_telemetry.h"

TelemetryStatusPage::TelemetryStatusPage(
    CephContext* _cct, const S3GWTelemetry& _telemetry
)
    : cct(_cct), telemetry(_telemetry) {}

TelemetryStatusPage::~TelemetryStatusPage() {}

http::status TelemetryStatusPage::render(std::ostream& os) {
  os << "<h1>Telemetry / Upgrades </h1>";
  // Versions available block
  auto versions = telemetry.available_versions();
  fmt::print(
      os,
      "<h2>Versions</h2>\n"
      "<p>Current version: {}</p>\n"
      "<p>Available: \n"
      "<ul>\n",
      ceph_version_to_str()
  );
  for (const auto& version : versions) {
    fmt::print(
        os,
	"<li>{} ({:%Y-%m-%d} UTC)</li>\n",
	version.name,
        fmt::gmtime(ceph::real_clock::to_time_t(version.release_date))
    );
  }
  os << "</ul>\n"
      "</p>\n";

  // Status block
  auto status = telemetry.status();
  fmt::print(
      os,
      "<h2>Status</h2>\n"
      "<ul>\n"
      "<li>Last attempt: {:%Y-%m-%d %H:%M:%S} UTC</li>\n"
      "<li>Last update: {:%Y-%m-%d %H:%M:%S} UTC</li>\n"
      "</ul>\n",
      fmt::gmtime(ceph::real_clock::to_time_t(status.last_attempt)),
      fmt::gmtime(ceph::real_clock::to_time_t(status.last_success))
  );

  // Request block
  JSONFormatter f(true);
  std::ostringstream request_os;
  const auto url =
      cct->_conf.get_val<std::string>("rgw_s3gw_telemetry_upgrade_responder_url"
      );
  telemetry.create_update_responder_request(&f);
  f.flush(request_os);
  fmt::print(
      os,
      "<h2>Request</h2>\n"
      "<p>Data s3gw sends periodically to <strong>{}</strong></p>\n"
      "<pre><code>{}</code></pre>\n",
      url, request_os.str()
  );

  return http::status::ok;
}
