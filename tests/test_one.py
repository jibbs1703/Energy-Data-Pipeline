import boto3
from botocore.exceptions import ClientError


def test_s3_connection():
    """
    Tests the connection to AWS S3.

    Returns:
      True if the connection is successful, False otherwise.
    """
    try:
        s3_client = boto3.client("s3")
        # Perform a simple operation to check connection
        response = s3_client.list_buckets()
        return True
    except ClientError as e:
        print(f"S3 connection failed: {e}")
        return False


if __name__ == "__main__":
    if test_s3_connection():
        print("Connected to S3 successfully!")
    else:
        print("Failed to connect to S3.")
