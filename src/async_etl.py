import logging
import aiohttp
import asyncio
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
        provinces = ["NSW"]  # ["NSW", "QLD", "VIC", "SA", "TAS"]
    for province in provinces:
        for year in years:
            for month in months:
                url = f"{base_url}_{year}{month:02}_{province}1.csv"
                yield url


async def get_data(url, session):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    async with session.get(url, headers=headers) as response:
        name = url.split("/")[-1]
        if response.status != 200:
            logger.error(f"Failed to fetch data: {name}")
            return None, None
        data = await response.text()
        data = StringIO(data)
        data = pd.read_csv(data)
        csv_file = StringIO()
        data.to_csv(csv_file, index=False)
        return csv_file, name


def write_to_s3(bucket_name, filename, file, folder=""):
    s3_conn.upload_file(
        bucket_name=bucket_name, filename=filename, file=file, folder=folder
    )


async def main():
    gen = get_url()
    async with aiohttp.ClientSession() as session:
        tasks = []
        while True:
            try:
                url = next(gen)
                tasks.append(get_data(url, session))
            except StopIteration:
                logger.info("All generated links have been yielded.")
                break
        results = await asyncio.gather(*tasks)
        for file, filename in results:
            if file:
                write_to_s3(
                    bucket_name="jibbs-machine-learning-bucket",
                    filename=filename,
                    file=file,
                    folder="AEMO_Price_Demand/",
                )
                logger.info(f"Data fetched: {filename}")


if __name__ == "__main__":
    asyncio.run(main())
