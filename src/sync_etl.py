import logging
import requests
from io import StringIO

import pandas as pd

from aws.s3 import S3Buckets

# Create a custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create handlers
console_handler = logging.StreamHandler()

# Set the level for handlers
console_handler.setLevel(logging.INFO)

# Create formatters and add them to handlers
console_format = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
console_handler.setFormatter(console_format)

# Add handlers to the logger
logger.addHandler(console_handler)

s3_conn = S3Buckets.credentials("us-east-2")
final_df = pd.DataFrame()


# Function to Extract Files from AEMO Website
def get_url(
    base_url="https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND",
    years=range(1999, 2000),
    months=range(1, 13),
    provinces=None,
):

    if provinces is None:
        provinces = ["QLD"]  # ["NSW", "QLD", "VIC", "SA", "TAS"]
    for province in provinces:
        for year in years:
            for month in months:
                url = f"{base_url}_{year}{month:02}_{province}1.csv"
                yield url


def get_data(url):
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


def write_to_s3(bucket_name, filename, file, folder=""):
    s3_conn.put_file_in_s3(
        bucket_name=bucket_name, filename=filename, file=file, folder=folder
    )


if __name__ == "__main__":
    gen = get_url()
    while True:
        try:
            file, filename = get_data(next(gen))
            write_to_s3(
                bucket_name="jibbs-machine-learning-bucket",
                filename=filename,
                file=file,
                folder="AEMO_Price_Demand/",
            )
            logger.info(f"Data fetched: {filename}")
        except StopIteration:
            logger.info("All generated links have been yielded.")
            break
        # Exit the loop if StopIteration is raised
        except Exception as e:
            logger.error(f"An error occurred: {e}")
