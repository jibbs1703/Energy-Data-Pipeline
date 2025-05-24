"""Energy Data Extraction and Load Async Module."""
import argparse
import asyncio
import datetime
from collections.abc import AsyncGenerator
from io import StringIO

import aiohttp
import pandas as pd

from energy_data.logs import get_logger
from energy_data.s3 import S3Buckets

logger = get_logger(__name__)
s3_conn = S3Buckets.credentials("us-east-2")


async def get_url(
    base_url: str,
    current_year: int,
    months: range,
    provinces: list[str],
) -> AsyncGenerator[str, None]:
    """
    Asynchronously generates URLs for the specified base URL, year, months, and provinces.
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


async def get_data(url: str) -> tuple[StringIO, str]:
    """
    Asynchronously fetches data from the given URL and returns it as a StringIO object.
    The data is read as a CSV file and then converted to a StringIO object.
    If the request fails, it returns an empty StringIO object and the name of the file.
    If the request is successful, it returns the StringIO object with the data and the name
    of the file.
    The name of the file is extracted from the URL.
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
    name = url.split("/")[-1]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    text = await response.text()
                    data = StringIO(text)
                    df = pd.read_csv(data)
                    csv_file = StringIO()
                    df.to_csv(csv_file, index=False)
                    return csv_file, name
                else:
                    logger.error(f"Failed to fetch data from {url}. Status code: {response.status}")
                    return StringIO(""), name
    except aiohttp.ClientError as e:
        logger.error(f"AIOHTTP error fetching {url}: {e}")
        return StringIO(""), name
    except pd.errors.ParserError as e:
        logger.error(f"An error occurred while parsing data from {url}: {e}")
        return StringIO(""), name


async def write_to_s3(bucket_name, filename, file, folder="") -> None:
    """
    Asynchronously uploads a file to an S3 bucket. Checks if the file already exists
    in the bucket before uploading.
    If the file already exists, it skips the upload.
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


async def process_url(url: str, bucket_name: str, folder: str) -> None:
    """
    Asynchronously fetches data from a URL and uploads it to S3.
    Args:
        url (str): The URL to fetch data from.
        bucket_name (str): The name of the S3 bucket.
        folder (str): The folder in the S3 bucket where the file will be uploaded.
    """
    file, filename = await get_data(url)
    if file.getvalue() == "":
        logger.error(f"Data not available: {filename}. Skipping upload.")
        return
    await write_to_s3(
        bucket_name=bucket_name,
        filename=filename,
        file=file,
        folder=folder,
    )


async def run_data_extraction(
    base_url: str,
    bucket_name: str,
    current_year: int,
    months: range,
    provinces: list[str],
    folder: str,
) -> None:
    """
    Main asynchronous function to run the data extraction and upload process.
    Args:
        base_url (str): The base URL for the data.
        bucket_name (str): The name of the S3 bucket.
        current_year (int): The current year.
        months (range): The range of months to generate URLs for.
        provinces (list): The list of provinces to generate URLs for.
        folder (str): The folder in the S3 bucket where the file will be uploaded.
    """
    async for url in get_url(base_url, current_year, months, provinces):
        await process_url(url, bucket_name, folder)
    logger.info("All generated links have been processed.")


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
    asyncio.run(
        run_data_extraction(
            base_url=args.base_url,
            bucket_name=args.bucket_name,
            current_year=args.current_year,
            months=args.months,
            provinces=args.provinces,
            folder=args.folder,
        )
    )
    logger.info("data extraction and upload to AWS S3 completed.")
