# Copyright 2023 SUSE, LLC.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
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
import datetime
import time

# ******************************************************************************
# This file is created meanwhile we don't have the s3tests forked.
# When s3tests are forked these tests should be moved to s3tests to avoid
# havind the same test in 2 places.
# ******************************************************************************


class CopyObjectTests(unittest.TestCase):
    ACCESS_KEY = "test"
    SECRET_KEY = "test"
    URL = "http://127.0.0.1:7480"
    BUCKET_NAME_LENGTH = 8
    OBJECT_NAME_LENGTH = 10

    def setUp(self):
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=CopyObjectTests.URL,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )

        self.s3 = boto3.resource(
            "s3",
            endpoint_url=CopyObjectTests.URL,
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
        return self.get_random_name(CopyObjectTests.BUCKET_NAME_LENGTH)

    def get_random_object_name(self) -> str:
        return self.get_random_name(CopyObjectTests.OBJECT_NAME_LENGTH)

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

    def upload_file_and_check(self, bucket_name, object_name, body=""):
        test_file_path = os.path.join(self.test_dir.name, object_name).replace("/", "_")
        if body == "":
            self.generate_random_file(test_file_path)
        else:
            with open(test_file_path, "wb") as fout:
                fout.write(body.encode())
        # upload the file
        self.s3_client.upload_file(test_file_path, bucket_name, object_name)

        # get the file and compare with the original
        test_file_path_check = os.path.join(self.test_dir.name, "test_file_check.bin")
        self.s3_client.download_file(bucket_name, object_name, test_file_path_check)
        self.assertTrue(
            filecmp.cmp(test_file_path, test_file_path_check, shallow=False)
        )

    def get_body(self, response):
        body = response["Body"]
        got = body.read()
        if type(got) is bytes:
            got = got.decode()
        return got

    def get_status_and_error_code(self, response):
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        error_code = response["Error"]["Code"]
        return status, error_code

    def test_copy_object_response(self):
        bucket_name = self.get_random_bucket_name()
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.assert_bucket_exists(bucket_name)
        original_object_name = "test_object_original"
        self.upload_file_and_check(bucket_name, "test_object_original")
        response = self.s3_client.copy_object(
            CopySource="{0}/{1}".format(bucket_name, original_object_name),
            Bucket=bucket_name,
            Key="test_object_copy",
        )
        self.assertTrue("CopyObjectResult" in response)
        self.assertTrue("ETag" in response["CopyObjectResult"])
        self.assertTrue("LastModified" in response["CopyObjectResult"])
        etag = response["CopyObjectResult"]["ETag"]
        self.assertNotEqual(0, len(etag))
        last_modified = response["CopyObjectResult"]["LastModified"]
        self.assertTrue(isinstance(last_modified, datetime.datetime))
        # ensure that the time is not 0 epoch
        self.assertNotEqual(1970, last_modified.year)
        current_time = datetime.datetime.now()
        # check that current time matches the last_modified one
        # (year should be the same or +1 if this test was executed exactly
        # during New Year's Eve)
        self.assertTrue(
            current_time.year == last_modified.year
            or current_time.year == last_modified.year + 1
        )

    def test_copy_object_ifmodifiedsince_good(self):
        bucket_name = self.get_random_bucket_name()
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.assert_bucket_exists(bucket_name)
        original_object_name = "test_object_original"
        self.upload_file_and_check(bucket_name, "test_object_original", "TEST")
        response = self.s3_client.copy_object(
            Bucket=bucket_name,
            CopySource=bucket_name + "/test_object_original",
            Key="bar",
            CopySourceIfModifiedSince="Sat, 29 Oct 1994 19:43:31 GMT",
        )
        response = self.s3_client.get_object(Bucket=bucket_name, Key="bar")
        self.assertTrue("Body" in response)
        self.assertEqual("TEST", self.get_body(response))

    def test_copy_object_ifmodifiedsince_failed(self):
        bucket_name = self.get_random_bucket_name()
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.assert_bucket_exists(bucket_name)
        self.s3_client.put_object(Bucket=bucket_name, Key="foo", Body="bar")
        response = self.s3_client.get_object(Bucket=bucket_name, Key="foo")
        last_modified = str(response["LastModified"])

        last_modified = last_modified.split("+")[0]
        mtime = datetime.datetime.strptime(last_modified, "%Y-%m-%d %H:%M:%S")

        after = mtime + datetime.timedelta(seconds=1)
        after_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", after.timetuple())

        time.sleep(1)

        with self.assertRaises(botocore.exceptions.ClientError) as cm:
            self.s3_client.copy_object(
                Bucket=bucket_name,
                CopySource=bucket_name + "/foo",
                CopySourceIfModifiedSince=after_str,
                Key="bar",
            )
        status, error_code = self.get_status_and_error_code(cm.exception.response)
        self.assertEqual(status, 412)
        self.assertEqual(error_code, "PreconditionFailed")

    def test_copy_object_ifunmodifiedsince_good(self):
        bucket_name = self.get_random_bucket_name()
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.assert_bucket_exists(bucket_name)
        self.s3_client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

        with self.assertRaises(botocore.exceptions.ClientError) as cm:
            self.s3_client.copy_object(
                Bucket=bucket_name,
                CopySource=bucket_name + "/foo",
                CopySourceIfUnmodifiedSince="Sat, 29 Oct 1994 19:43:31 GMT",
                Key="bar",
            )
        status, error_code = self.get_status_and_error_code(cm.exception.response)
        self.assertEqual(status, 412)
        self.assertEqual(error_code, "PreconditionFailed")

    def test_copy_object_ifunmodifiedsince_failed(self):
        bucket_name = self.get_random_bucket_name()
        self.s3_client.create_bucket(Bucket=bucket_name)
        self.assert_bucket_exists(bucket_name)
        self.s3_client.put_object(Bucket=bucket_name, Key="foo", Body="bar")

        response = self.s3_client.copy_object(
            Bucket=bucket_name,
            CopySource=bucket_name + "/foo",
            Key="bar",
            CopySourceIfUnmodifiedSince="Sat, 29 Oct 2100 19:43:31 GMT",
        )
        response = self.s3_client.get_object(Bucket=bucket_name, Key="bar")
        body = self.get_body(response)
        self.assertEqual("bar", body)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        address_port = sys.argv.pop()
        CopyObjectTests.URL = "http://{0}".format(address_port)
        unittest.main()
    else:
        print("usage: {0} ADDRESS:PORT".format(sys.argv[0]))
