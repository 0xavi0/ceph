// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <sstream>

#include "common/Formatter.h"
#include "common/ceph_context.h"
#include "common/ceph_json.h"
#include "rgw_common.h"
#include "rgw_s3gw_telemetry.h"
#include "rgw_sal_sfs.h"


class MockSFStore : rgw::sal::SFStore {
};

class TestS3GWTelemetry : public ::testing::Test {
 protected:
  CephContext* cct;
  std::unique_ptr<S3GWTelemetry> uut;

  void SetUp() override {
    cct = (new CephContext(CEPH_ENTITY_TYPE_ANY))->get();
    uut = std::make_unique<S3GWTelemetry>(cct, nullptr);
  }
};

TEST_F(TestS3GWTelemetry, parses_valid_response) {
  bufferlist bl;
  bl.append(R"json(
  {
    "requestIntervalInMinutes": 60,
    "versions": [
        {
            "ExtraInfo": null,
            "MinUpgradableVersion": "",
            "Name": "v0.23.42",
            "ReleaseDate": "2023-03-09T12:00:00Z",
            "Tags": [
                "v0.23.42",
                "latest"
            ]
        }
    ]
})json");

  auto maybe_parsed = uut->parse_upgrade_response(bl);
  ASSERT_TRUE(maybe_parsed.has_value());
  auto parsed = maybe_parsed.value();
  ASSERT_EQ(parsed.size(), 1);
  ASSERT_EQ(parsed[0].name, "v0.23.42");
  ASSERT_GT(parsed[0].release_date.time_since_epoch().count(), 0);
}

TEST_F(TestS3GWTelemetry, broken_json_responses_return_nullopt) {
  bufferlist bl;
  bl.append("{{{{ ~~~ BROKEN JSON]]]");

  auto maybe_parsed = uut->parse_upgrade_response(bl);
  ASSERT_FALSE(maybe_parsed.has_value());
}

TEST_F(TestS3GWTelemetry, invalid_json_responses_return_nullopt) {
  bufferlist bl;
  bl.append(R"json({
    "wat?": [23, 42]
})json");

  ASSERT_FALSE(uut->parse_upgrade_response(bl).has_value());

  bl.clear();
  bl.append(R"json({
    "versions": [23, 42]
})json");
  ASSERT_FALSE(uut->parse_upgrade_response(bl).has_value());

}

TEST_F(TestS3GWTelemetry, creates_valid_updateresponder_json_request) {
  JSONFormatter f;
  std::ostringstream os;
  uut->create_update_responder_request(&f);
  f.flush(os);
  std::string rendered = os.str();

  JSONParser parser;
  ASSERT_TRUE(parser.parse(rendered.c_str(), rendered.length()));
  ASSERT_TRUE(parser.is_object());

  auto appVersionIter = parser.find_first("appVersion");
  ASSERT_FALSE(appVersionIter.end());
  auto appVersion = *appVersionIter;
  ASSERT_FALSE(appVersion->get_data().empty());

  auto extraInfo = parser.find_obj("extraInfo");
  ASSERT_TRUE(extraInfo != nullptr);
}
