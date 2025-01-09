import requests
from io import StringIO

import pandas as pd

from aws_s3 import S3Buckets

s3_conn = S3Buckets.credentials('us-east-2')
final_df = pd.DataFrame()

# Function to Extract Files from AEMO Website
def get_url(base_url="https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND",
            years=range(1999,2025), months = range(1,13),
            provinces=None):

    if provinces is None:
        provinces = ["NSW", "QLD", "VIC", "SA", "TAS"]
    for province in provinces:
        for year in years:
            for month in months:
                url=f"{base_url}_{year}{month:02}_{province}1.csv"
                yield url

def get_data(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(url, headers=headers)
    name = url.split('/')[-1]

    if response.status_code == 200:
        data = StringIO(response.text)
        dataframe = pd.read_csv(data)

        return dataframe,name

def write_to_s3(df, bucket_name, file_name, folder=''):
    s3_conn.upload_dataframe_to_s3(df=df, bucket_name=bucket_name, object_name=f'{folder}{filename}')

if __name__ == "__main__":
    # To Write Individual CSV Files
    gen = get_url()
    for num in range(12):
        df, filename = get_data(next(gen))
        write_to_s3(df=df, bucket_name='jibbs-machine-learning-bucket', folder= 'AEMO_Price_Demand/', file_name=filename)

    # # To Aggregate All CSV Files
    # final_df = pd.DataFrame()
    # gen = get_url()
    # for num in range(12):
    #     df, filename = get_data(next(gen))
    #     print(filename)
    #     final_df = pd.concat([final_df, df])
    # write_to_s3(df=final_df, bucket_name='jibbs-machine-learning-bucket', folder= 'AEMO_Price_Demand/', filename='aggregate.csv')