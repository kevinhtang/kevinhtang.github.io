import logging
import os
import boto3

from botocore.client import BaseClient
from botocore.exceptions import ClientError


class S3Client:
    def __init__(self, s3: BaseClient, bucket_name: str):
        """
        A wrapper class around an s3 client.

        Arguments:
            s3 (BaseClient): The underlying s3 client.
            bucket_name (str): The s3 bucket to access.
        """
        self.bucket_name = bucket_name
        self.s3 = s3

    def upload_file(self, file_path: str, s3_key: str) -> None:
        """
        Upload a file to the s3 bucket.

        Arguments:
            file_path (str): Path to local file.
            s3_key (str): Key where the file should be uploaded to.
        """
        try:
            self.s3.upload_file(file_path, self.bucket_name, s3_key)
            logging.debug(f"File {file_path} uploaded to {self.bucket_name}/{s3_key}")
        except ClientError as e:
            logging.error(f"Error uploading file to {self.bucket_name}/{s3_key}: {e}")
            raise e

    def download_file(
        self, s3_key: str, file_path: str, exist_ok: bool = False
    ) -> None:
        """
        Download a file from the s3 bucket.

        Arguments:
            s3_key (str): Key where the file is located in the bucket.
            file_path (str): Local path to download the file to.
            exist_ok (bool): If False and the file exists at file_path, raise an Exception. Otherwise,
                log a warning if the file_path exists.
        """
        if os.path.exists(file_path):
            if exist_ok:
                logging.warning(f"File exists: {file_path}. This will get overwritten.")
            else:
                raise FileExistsError(f"File exists: {file_path}")

        try:
            self.s3.download_file(self.bucket_name, s3_key, file_path)
            logging.info(f"File {self.bucket_name}/{s3_key} downloaded to {file_path}")
        except ClientError as e:
            logging.error(f"Error downloading file {self.bucket_name}/{s3_key}: {e}")
            raise e

    def key_exists(self, s3_key) -> bool:
        """
        Check whether a key exists in the s3 bucket.

        Arguments:
            s3_key (str): Key to check.

        Returns
            - (bool) Wheter the key exists.
        """
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False

            logging.error(
                f"Error checking whether key {self.bucket_name}/{s3_key} exists: {e}"
            )
            raise e