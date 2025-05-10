"""Energy Data Extraction and Load Sync Module."""

import argparse
import datetime
from collections.abc import Generator
from io import StringIO

import pandas as pd
import requests
from aws.s3 import S3Buckets
from logs import get_logger

logger = get_logger(__name__)
s3_conn = S3Buckets.credentials("us-east-2")


def get_url(
    base_url: str,
    current_year: int,
    months: range,
    provinces: list[str],
) -> Generator[str]:
    """
    Generates URLs for the specified base URL, year, months, and provinces.
    Args:
        base_url (str): The base URL for the data.
        current_year (int): The current year.
        months (range): The range of months to generate URLs for.
        provinces (list): The list of provinces to generate URLs for.
    Yields:
        str: The generated URL.
    """
    for province in provinces:
        for year in range(1998, current_year + 1):
            for month in months:
                url = f"{base_url}_{year}{month:02}_{province}1.csv"
                yield url


def get_data(url: str) -> tuple[StringIO, str]:
    """
    Fetches data from the given URL and returns it as a StringIO object.
    The data is read as a CSV file and then converted to a StringIO object.
    If the request fails, it returns an empty StringIO object and the name of the file.
    If the request is successful, it returns the StringIO object with the data and the
    name of the file. The name of the file is extracted from the URL.
    Args:
        url (str): The URL to fetch data from.
    Returns:
        tuple: A tuple containing the StringIO object with the data and the name of the file.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers, timeout=10)
    name = url.split("/")[-1]

    if response.status_code == 200:
        data = StringIO(response.text)
        data = pd.read_csv(data)
        csv_file = StringIO()
        data.to_csv(csv_file, index=False)
        return csv_file, name
    else:
        return StringIO(""), name


def write_to_s3(bucket_name, filename, file, folder="") -> None:
    """
    Uploads a file to an S3 bucket. Checks if the file already exists in the bucket before
    uploading. If the file already exists, it skips the upload.
    If the file does not exist, it uploads the file to the specified folder in the S3 bucket.
    Args:
        bucket_name (str): The name of the S3 bucket.
        filename (str): The name of the file to be uploaded.
        file (StringIO): The file object to be uploaded.
        folder (str): The folder in the S3 bucket where the file will be uploaded.
    """
    try:
        previously_uploaded = s3_conn.list_files(bucket_name=bucket_name, folder=folder)
        if f"{folder}{filename}" in previously_uploaded:
            logger.info(f"File '{filename}' already exists in '{bucket_name}'. Skipping upload.")
            return
        logger.info(f"Uploading '{filename}' to S3 bucket '{bucket_name}'...")
        s3_conn.upload_file(bucket_name=bucket_name, filename=filename, file=file, folder=folder)
    except (s3_conn.S3UploadError, s3_conn.S3ConnectionError) as e:
        logger.error(f"Failed to upload '{filename}' to '{bucket_name}': {e}")


def run_data_extraction(
    base_url: str,
    bucket_name: str,
    current_year: int,
    months: range,
    provinces: list[str],
    folder: str,
) -> None:
    """
    Main function to run the data extraction and upload process.
    Args:
        base_url (str): The base URL for the data.
        bucket_name (str): The name of the S3 bucket.
        current_year (int): The current year.
        months (range): The range of months to generate URLs for.
        provinces (list): The list of provinces to generate URLs for.
        folder (str): The folder in the S3 bucket where the file will be uploaded.
    """
    gen = get_url(base_url, current_year, months, provinces)
    for _ in gen:
        try:
            file, filename = get_data(next(gen))
            if file.getvalue() == "":
                logger.error(f"Data not available: {filename}. Skipping upload.")
                continue
            write_to_s3(
                bucket_name=bucket_name,
                filename=filename,
                file=file,
                folder=folder,
            )
        except StopIteration:
            logger.info("All generated links have been yielded.")
            break
        except (requests.RequestException, pd.errors.ParserError, s3_conn.S3UploadError) as e:
            logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Extraction Arguments.")
    parser.add_argument(
        "--base_url",
        type=str,
        help="Base URL for the data.",
        default="https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND",
    )
    parser.add_argument(
        "--bucket_name",
        type=str,
        default="energy-data-bucket",
        help="S3 bucket name.",
    )
    parser.add_argument(
        "--current_year",
        type=int,
        default=datetime.datetime.now().year,
        help="Current year.",
    )
    parser.add_argument(
        "--folder",
        type=str,
        default="Energy_Price_Demand/",
        help="Folder to upload data to in AWS S3.",
    )
    parser.add_argument(
        "--months",
        type=int,
        nargs="+",
        default=list(range(1, 13)),
        help="Months to fetch data for.",
    )
    parser.add_argument(
        "--provinces",
        type=str,
        help="Provinces to fetch data for.",
        nargs="+",
        default=["QLD", "NSW", "VIC", "SA", "TAS"],
    )
    args = parser.parse_args()
    logger.info(f"Arguments: {args}")
    logger.info("Starting data extraction and upload process...")
    run_data_extraction(
        base_url=args.base_url,
        bucket_name=args.bucket_name,
        current_year=args.current_year,
        months=args.months,
        provinces=args.provinces,
        folder=args.folder,
    )
    logger.info("Data extraction and upload to AWS S3 completed.")
