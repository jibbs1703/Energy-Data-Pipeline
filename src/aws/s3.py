import os
from dotenv import load_dotenv
import logging
import boto3


class S3Buckets:
    @classmethod
    def credentials(cls, region=None):
        """
        Retrieves AWS credentials from a hidden environment file.

        This class method accesses the user's AWS secret and access keys stored in an environment file.
        If a region is specified, the methods within the S3Buckets class will execute in that region.
        Otherwise, AWS will assign a default region.

        :param region: AWS region specified by the user (default is None)
        :return: An instance of the S3Buckets class initialized with the user's credentials and specified region
        """
        load_dotenv()
        secret = os.getenv("ACCESS_SECRET")
        access = os.getenv("ACCESS_KEY")

        return cls(secret, access, region)

    def __init__(self, secret, access, region):
        """
        Initializes the S3Buckets class with user credentials and creates the AWS S3 client.

        This constructor method initializes the S3Buckets class using the provided secret and access keys.
        It creates an AWS S3 client using the boto3 library. If no region is specified, AWS assigns a default
        region. The created client is available for subsequent methods within the class.

        :param secret: User's AWS secret key loaded from the environment file
        :param access: User's AWS access key loaded from the environment file
        :param region: Specified AWS region during instantiation (default is None)
        """
        if region is None:
            self.client = boto3.client(
                "s3", aws_access_key_id=access, aws_secret_access_key=secret
            )
            print(secret, access, region)
        else:
            self.location = {"LocationConstraint": region}
            self.client = boto3.client(
                "s3",
                aws_access_key_id=access,
                aws_secret_access_key=secret,
                region_name=region,
            )

    def list_buckets(self):
        """
        Retrieves and returns a list of bucket names available in the user's AWS account.

        :return: A list of the S3 bucket instances present in the accessed account
        """
        response = self.client.list_buckets()
        buckets = response["Buckets"]
        all_buckets = [bucket["Name"] for bucket in buckets]
        return all_buckets

    def create_bucket(self, bucket_name):
        """
        Creates an S3 bucket in the user's AWS account.

        This method creates a new S3 bucket in the region specified during the instantiation of the class.
        If the bucket name has already been used, it will not create a new bucket. If no region is specified,
        the bucket is created in the default S3 region (us-east-1).

        :param bucket_name: Name of the bucket to be created
        """
        if bucket_name in self.list_buckets():
            print(f"The bucket {bucket_name} already exists")
            pass
        else:
            print("A new bucket will be created in your AWS account")
            self.client.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration=self.location
            )
            print(f"The bucket {bucket_name} has been successfully created")

    def put_file_in_s3(self, bucket_name, filename, file, folder=""):
        try:
            self.client.put_object(
                Bucket=bucket_name, Key=f"{folder}{filename}", Body=file.getvalue()
            )
        except Exception as e:
            logging.error(f"Error uploading file to S3: {str(e)}")
