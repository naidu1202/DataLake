# DataLake

To improve  website personalization platform, to comeup with a data pipeline in data platform to handle the large-scale data. Data is being retrieving from web page and mobile app via FTP in a daily batch

The below are the steps followed for orechestrating the data pipeline.

# DataLake

1. upload the recieved data to S3
2. process the data using spark application
  a) While reading the files, check if it is already processed
  b) Apply data cleaning 
  c) create a parquet file
  d) move the input and intermediate files to archive
3. deploy the objects to the snowflake to maintain the data
4. load the data using airflow into snowflake
4. enable the scheduler to orechestrate above steps.


