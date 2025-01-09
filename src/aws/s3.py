import os
from io import StringIO
from dotenv import load_dotenv
import logging
import boto3
from botocore.exceptions import ClientError

# Create Logger
logger = logging.getLogger(__name__)

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
            self.client = boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret)
            print(secret, access, region)
        else:
            self.location = {'LocationConstraint': region}
            self.client = boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret, region_name=region)

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
            self.client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=self.location)
            print(f"The bucket {bucket_name} has been successfully created")

    def upload_file(self, file_name, bucket_name, object_name=None):
        """
        Uploads a file to an S3 bucket in the user's AWS account.

        :param file_name: Name of the file to upload to the S3 bucket
        :param bucket_name: Name of the bucket to upload the file to
        :param object_name: S3 object name. If not specified, the file_name is used
        :return: True if the file was uploaded successfully, else False
        """
        if object_name is None:
            object_name = os.path.basename(file_name)
        try:
            self.client.upload_file(file_name, bucket_name, object_name)

        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_file(self, bucket_name, object_name, file_name):
        """
        Downloads a file from an S3 bucket in the user's AWS account.

        :param bucket_name: Name of the bucket to download the file from
        :param object_name: Name of the file to download from the S3 bucket
        :param file_name: Name of the file to save the downloaded content to
        :return: None
        """
        file = self.client.download_file(bucket_name, object_name, file_name)
        return file

    def delete_file(self, bucket_name, file_name):
        """
        Deletes a file from an S3 bucket in the user's AWS account.

        :param bucket_name: Name of the bucket to access the file
        :param file_name: Name of the file to delete from the S3 bucket

        :return: None
        """
        self.client.delete_object(Bucket=bucket_name, Key=file_name)
        return f"The file {file_name} has been deleted from the bucket {bucket_name}"

    def read_file(self, bucket_name, object_name):
        """
        Reads a file from an S3 bucket in the user's AWS account and returns its contents.

        :param bucket_name: Name of the bucket to read the file from
        :param object_name: Name of the file to read from the S3 bucket
        :return: An object containing the file's contents
        """
        response = self.client.get_object(Bucket=bucket_name, Key=object_name)
        file = StringIO(response['Body'].read().decode('utf-8'))
        return file

    def upload_dataframe_to_s3(self, df, bucket_name, object_name):
        """
        Uploads a pandas DataFrame to an S3 bucket in the user's AWS account as a CSV file.

        :param df: DataFrame to upload to the S3 bucket
        :param bucket_name: Name of the bucket to upload the DataFrame to
        :param object_name: Name the DataFrame will take in the user's S3 bucket
        :return: None
        """
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, header=True, index=False)
        self.client.put_object(Bucket=bucket_name, Body=csv_buffer.getvalue(), Key=object_name)
        print("Dataframe is saved as CSV in S3 bucket.")