#!/usr/bin/env python3
#
# Copyright 2023 SUSE, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
import sys
import boto3, botocore
import random
import string
import tempfile
import os
import filecmp
import time


class LifecycleSmokeTests(unittest.TestCase):
    ACCESS_KEY = "test"
    SECRET_KEY = "test"
    URL = "http://127.0.0.1:7480"
    BUCKET_NAME_LENGTH = 8
    OBJECT_NAME_LENGTH = 10
    LIFECYCLE_DEBUG_INTERVAL = 10

    def setUp(self):
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=LifecycleSmokeTests.URL,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )

        self.s3 = boto3.resource(
            "s3",
            endpoint_url=LifecycleSmokeTests.URL,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )

        self.test_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.s3_client.close()
        self.test_dir.cleanup()

    def get_random_name(self, length) -> str:
        letters = string.ascii_lowercase
        result_str = "".join(random.choice(letters) for i in range(length))
        return result_str

    def get_random_bucket_name(self) -> str:
        return self.get_random_name(LifecycleSmokeTests.BUCKET_NAME_LENGTH)

    def get_random_object_name(self) -> str:
        return self.get_random_name(LifecycleSmokeTests.OBJECT_NAME_LENGTH)

    def generate_random_file(self, path, size=4):
        # size passed is in mb
        size = size * 1024 * 1024
        with open(path, "wb") as fout:
            fout.write(os.urandom(size))

    def assert_bucket_exists(self, bucket_name):
        response = self.s3_client.list_buckets()
        found = False
        for bucket in response["Buckets"]:
            if bucket["Name"] == bucket_name:
                found = True
        self.assertTrue(found)

    def upload_file_and_check(self, bucket_name, object_name):
        test_file_path = os.path.join(self.test_dir.name, object_name.replace("/", "_"))
        self.generate_random_file(test_file_path)
        # upload the file
        self.s3_client.upload_file(test_file_path, bucket_name, object_name)

        # get the file and compare with the original
        test_file_path_check = os.path.join(self.test_dir.name, "test_file_check.bin")
        self.s3_client.download_file(bucket_name, object_name, test_file_path_check)
        self.assertTrue(
            filecmp.cmp(test_file_path, test_file_path_check, shallow=False)
        )

    def check_objects_exist(self, objects, objects_expected):
        objects_found = 0
        for obj in objects:
            if obj["Key"] in objects_expected:
                objects_found += 1
        self.assertEqual(len(objects_expected), objects_found)
        self.assertEqual(len(objects_expected), len(objects))

    def create_random_buckets(self, num_buckets):
        buckets = []
        for i in range(num_buckets):
            bucket_name = self.get_random_bucket_name()
            buckets.append(bucket_name)
            self.s3_client.create_bucket(Bucket=bucket_name)
            self.assert_bucket_exists(bucket_name)
        return buckets

    def test_expiration(self):
        bucket_name = self.get_random_bucket_name()
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.assert_bucket_exists(bucket_name)
        objects = [
            "expire1/foo",
            "expire1/bar",
            "keep2/foo",
            "keep2/bar",
            "expire3/foo",
            "expire3/bar",
        ]
        for obj in objects:
            self.upload_file_and_check(bucket_name, obj)

        response = self.s3_client.list_objects(Bucket=bucket_name)
        self.check_objects_exist(response["Contents"], objects)

        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 1},
                "Filter": {"Prefix": "expire1/"},
                "Status": "Enabled",
            },
            {
                "ID": "rule2",
                "Expiration": {"Days": 5},
                "Filter": {"Prefix": "expire3/"},
                "Status": "Enabled",
            },
        ]
        lifecycle = {"Rules": rules}
        self.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle
        )

        # give enough time to expire.
        # 3 cycles because:
        #                   1st cycle won't be expired yet (not still 1 day)
        #                   2nd cycle rgw considers the bucket at processed
        #                       today and skips it
        #                   3rd cycle will be fully expired
        time.sleep(3 * LifecycleSmokeTests.LIFECYCLE_DEBUG_INTERVAL)
        response = self.s3_client.list_objects(Bucket=bucket_name)
        self.check_objects_exist(
            response["Contents"],
            ["keep2/foo", "keep2/bar", "expire3/foo", "expire3/bar"],
        )

        time.sleep(LifecycleSmokeTests.LIFECYCLE_DEBUG_INTERVAL)
        # at this point there are still not enough cycles to count 5 days
        response = self.s3_client.list_objects(Bucket=bucket_name)
        self.check_objects_exist(
            response["Contents"],
            ["keep2/foo", "keep2/bar", "expire3/foo", "expire3/bar"],
        )

        # same logic as above for number of cycles plus
        time.sleep(3 * LifecycleSmokeTests.LIFECYCLE_DEBUG_INTERVAL)
        response = self.s3_client.list_objects(Bucket=bucket_name)
        self.check_objects_exist(response["Contents"], ["keep2/foo", "keep2/bar"])

    def test_lifecycle_versioning_enabled(self):
        bucket_name = self.get_random_bucket_name()
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.assert_bucket_exists(bucket_name)
        response = self.s3_client.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"MFADelete": "Disabled", "Status": "Enabled"},
        )
        test_file_path_1 = os.path.join(self.test_dir.name, "test_file_1.bin")
        self.generate_random_file(test_file_path_1)
        # upload the file
        self.s3_client.upload_file(test_file_path_1, bucket_name, "expire1/test")
        # now upload again with different content
        test_file_path_2 = os.path.join(self.test_dir.name, "test_file_2.bin")
        self.generate_random_file(test_file_path_2)
        self.s3_client.upload_file(test_file_path_2, bucket_name, "expire1/test")
        response = self.s3_client.list_object_versions(Bucket=bucket_name, Prefix="")
        self.assertTrue("Versions" in response)
        self.assertEqual(2, len(response["Versions"]))
        self.assertFalse("DeleteMarkers" in response)

        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 1},
                "Filter": {"Prefix": "expire1/"},
                "Status": "Enabled",
            }
        ]
        lifecycle = {"Rules": rules}
        self.s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle
        )

        # give enough time to expire.
        # 3 cycles because:
        #                   1st cycle won't be expired yet (not still 1 day)
        #                   2nd cycle rgw considers the bucket at processed
        #                       today and skips it
        #                   3rd cycle will be fully expired
        time.sleep(3 * LifecycleSmokeTests.LIFECYCLE_DEBUG_INTERVAL)
        response = self.s3_client.list_object_versions(Bucket=bucket_name, Prefix="")
        self.assertTrue("Versions" in response)
        self.assertEqual(2, len(response["Versions"]))
        self.assertTrue("DeleteMarkers" in response)
        self.assertEqual(1, len(response["DeleteMarkers"]))

    def test_expiration_multiple_buckets(self):
        buckets = self.create_random_buckets(10)
        objects = [
            "expire1/foo",
            "expire1/bar",
            "keep2/foo",
            "keep2/bar",
            "expire3/foo",
            "expire3/bar",
        ]
        rules = [
            {
                "ID": "rule1",
                "Expiration": {"Days": 1},
                "Filter": {"Prefix": "expire1/"},
                "Status": "Enabled",
            }
        ]
        lifecycle = {"Rules": rules}
        for bucket in buckets:
            for obj in objects:
                self.upload_file_and_check(bucket, obj)

        for bucket in buckets:
            response = self.s3_client.list_objects(Bucket=bucket)
            self.check_objects_exist(response["Contents"], objects)

        for bucket in buckets:
            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket, LifecycleConfiguration=lifecycle
            )

        # give enough time to expire.
        # 3 cycles because:
        #                   1st cycle won't be expired yet (not still 1 day)
        #                   2nd cycle rgw considers the bucket at processed
        #                       today and skips it
        #                   3rd cycle will be fully expired
        time.sleep(3 * LifecycleSmokeTests.LIFECYCLE_DEBUG_INTERVAL)
        for bucket in buckets:
            response = self.s3_client.list_objects(Bucket=bucket)
            self.check_objects_exist(
                response["Contents"],
                ["keep2/foo", "keep2/bar", "expire3/foo", "expire3/bar"],
            )


if __name__ == "__main__":
    if len(sys.argv) == 2:
        address_port = sys.argv.pop()
        LifecycleSmokeTests.URL = "http://{0}".format(address_port)
        unittest.main()
    else:
        print("usage: {0} ADDRESS:PORT".format(sys.argv[0]))
