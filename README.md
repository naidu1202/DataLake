# DataLake

Project - Data Lake

To improve  website personalization platform, to comeup with a data pipeline in data platform to handle the large-scale data. Data is being retrieving from web page and mobile app via FTP in a daily batch

Data recieves and uploaded to S3 in json files. Build an ETL pipeline for a data lake hosted on S3. We will load data from S3, process the data into analytics tables using Spark, and load them into snowflake for Data science team consumption. We will deploy this Spark process on a cluster using AWS.

Deployement

Read data from S3

searches data: <<bucket_name>>/searches_data
visitors data: <<bucket_name>>/visitors_data

Process data using spark

Transforms them with basic cleaning and business rules 

Writes them to partitioned parquet files in table directories on S3.

Load data using Airflow

processed data is loaded to snowflake

Build the model for reports.

Model is built for the reports
