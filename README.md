# Energy-Data-Ingestion-Pipeline

## Overview

This repository contains an ETL pipeline that fetches monthly electricity price and demand data by 
region in Australia. The source files are in CSV format and contain energy price and demand data at
30-minute intervals. The pipeline extracts the data from the Australian Energy Market Operator (AEMO)
website and uploads the data to an AWS S3 bucket, which serves as the landing location for the data.
The project includes asynchronous functions to handle multiple requests concurrently, ensuring 
efficient data fetching and processing.

Post-extraction, the data is transformed and loaded into an AWS SQL/NoSQL Database from the landing
AWS S3 Bucket (TBD)


## Project Components

- **Data Extraction**: The `get_url` function generates URLs for the desired data files based on specified 
parameters such as year, month, and province. The `get_data` function asynchronously fetches the data from
these URLs and returns the data in CSV format.

- **Data Upload**: The `write_to_s3` function establishes a connection to an AWS S3 bucket using credentials,
enabling the upload of fetched data files to the specified S3 bucket.

- **Logging Setup**: A custom logger is configured to capture and display logging information. The logger
is set to the `INFO` level and streams log messages to the console.


## Project Dependencies

To install the required dependencies, run:

```sh
pip install -r requirements.txt
```

