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
#ifndef RGW_STATUS_PAGE_TELEMETRY_H
#define RGW_STATUS_PAGE_TELEMETRY_H

#include <string>

#include "rgw_s3gw_telemetry.h"
#include "rgw_status_page.h"

class TelemetryStatusPage : public StatusPage {
 private:
  CephContext* cct;
  const S3GWTelemetry& telemetry;

 public:
  TelemetryStatusPage(CephContext* cct, const S3GWTelemetry& telemetry);
  virtual ~TelemetryStatusPage() override;
  std::string name() const override { return "Telemetry"; };
  std::string prefix() const override { return "/telemetry"; };
  std::string content_type() const override { return "text/html"; };
  http::status render(std::ostream& os) override;
};

#endif  // RGW_STATUS_PAGE_TELEMETRY_H
