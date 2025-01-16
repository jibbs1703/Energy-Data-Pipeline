from src.aws.s3 import S3Buckets
from botocore.exceptions import ClientError


def test_s3_connection():
    """Simple AWS S3 connection test"""
    try:
        s3_connection = S3Buckets.credentials("us-east-2")
        s3_connection.list_buckets()
        assert True
    except ClientError as e:
        print(f"S3 connection failed: {e}")
        assert False
