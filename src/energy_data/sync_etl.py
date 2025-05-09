import argparse
import datetime
import requests
from io import StringIO

import pandas as pd

from logs import get_logger
from aws.s3 import S3Buckets

logger = get_logger(__name__)
s3_conn = S3Buckets.credentials("us-east-2")
final_df = pd.DataFrame()


def get_url(
    current_year: int,
    base_url="https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND",
    months: list =range(1, 13),
    provinces=None,
):
    years=range(1998, current_year+1),
    if provinces is None:
        provinces = ["QLD", "NSW", "QLD", "VIC", "SA", "TAS"]
    for province in provinces:
        for year in years:
            for month in months:
                url = f"{base_url}_{year}{month:02}_{province}1.csv"
                yield url


def get_data(url: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    name = url.split("/")[-1]

    if response.status_code == 200:
        data = StringIO(response.text)
        data = pd.read_csv(data)
        csv_file = StringIO()
        data.to_csv(csv_file, index=False)
        return csv_file, name
    else:
        return StringIO(""), name

def write_to_s3(bucket_name, filename, file, folder=""):
    s3_conn.upload_file(
        bucket_name=bucket_name, filename=filename, file=file, folder=folder
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL arguments.")
    parser.add_argument(
        "--current_year",
        type=int,
        nargs="+",
        default=datetime.datetime.now().year,
        help="Current year.",
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
        nargs="+",
        default=["QLD", "NSW", "QLD", "VIC", "SA", "TAS"],
        help="Provinces to fetch data for.",
    )
    args = parser.parse_args()
    logger.info(f"Fetching data for years: {args.current_year}, months: {args.months}, provinces: {args.provinces}")
    gen = get_url()
    while True:
        try:
            file, filename = get_data(next(gen))
            if file.getvalue() == "":
                logger.error(f"Data not available: {filename}. Skipping upload.")
                continue
            write_to_s3(
                bucket_name="jibbs-machine-learning-bucket",
                filename=filename,
                file=file,
                folder="Energy_Price_Demand/",)
            logger.info(f"Data fetched: {filename}")
        except StopIteration:
            logger.info("All generated links have been yielded.")
            break
        except Exception as e:
            logger.error(f"An error occurred: {e}")
