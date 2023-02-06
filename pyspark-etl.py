import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *
import boto3,json
import pandas as pd

s3 = boto3.client('s3')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
    
    
def searches_table_clean(search_table):
    df_pandas= search_table.toPandas()
    return(df_pandas.fillna (value={'destination_out':'NNN','origin_out':'NNN','origin_out':99,'flight_date_outbound':'1800-01-01','flight_date_inbound':'1800-01-01'}))

def visitors_table_clean(visitors_table):
    df_pandas= visitors_table.toPandas()
    replace_values = {'false' : 0, 'true' : 1 } 
    df_pandas=df_pandas.replace({"registered":replace_values})
    return(df_pandas.fillna (value={'hits_avg':9999,'country':'NNN','region':'NN','first_hit_pagename':'NNNN','countPerday':99}))
    

def process_searches_data(spark, input_data, output_data):
    """
    Description:
        Process the searches data files of json format and create parquet files  for search table.
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """

    # get filepath to searches data file
    searches_data = input_data + "searches/*"

    # read searches data file
    df = spark.read.json(searches_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract required columns to create searches table
    searches_table = df.withColumn("hour",hour("date_time"))\
                    .withColumn("day",dayofmonth("date_time"))\
                    .withColumn("week",weekofyear("date_time"))\
                    .withColumn("month",month("date_time"))\
                    .withColumn("year",year("date_time"))\
                    .withColumn("weekday",dayofweek("date_time"))\
                    .select("visitor_id","date_time","hour", "day", "week", "month", "year", "weekday","flight_date_outbound","destination_out","flight_date_inbound","origin_ret","segments").drop_duplicates()
    
    search_clean=spark.createDataFrame(searches_table_clean(searches_table))
    
    #print(search_clean)

    # write searches table to parquet files 
    search_clean.write.parquet(output_data + "searches-out/", mode="overwrite")
    
def process_visitors_data(spark, input_data, output_data):
    """
    Description:
            Process the visits log file and extract data for table time, users and search-visits from it.
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """

    # get filepath to visits data file
    visits_data = os.path.join(input_data, "visitors/*")

    # read visits data file
    df = spark.read.json(visits_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()
    
    # extract required columns to create searches table
    visitors_table = df.withColumn("hour",hour("visit_start"))\
                    .withColumn("day",dayofmonth("visit_start"))\
                    .withColumn("week",weekofyear("visit_start"))\
                    .withColumn("month",month("visit_start"))\
                    .withColumn("year",year("visit_start"))\
                    .withColumn("weekday",dayofweek("visit_start"))\
                    .select("visitor_id","visit_start","hour", "day", "week", "month", "year", "weekday","hits_avg","visits","registered","logged_in","country","region","first_hit_pagename","countPerday").drop_duplicates()

    visitors_clean=spark.createDataFrame(visitors_table_clean(visitors_table))
    
    # write searches table to parquet files 
    visitors_clean.write.parquet(output_data + "visitors-out/", mode="overwrite")

    # extract columns for users table
    #users_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()

    # write users table to parquet files
    #users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")


    # extract columns to create time table
    

    # write time table to parquet files partitioned by year and month
    #time_table.write.parquet(os.path.join(output_data, "visitors-out/"), mode='overwrite')

    # read in song data to use for songplays table
    #searches_df = spark.read\
    #            .format("parquet")\
    #            .option("basePath", os.path.join(output_data, "searches-out/"))\
    #            .load(os.path.join(output_data, "searches-out/*/*/"))

    # extract columns from joined song and log datasets to create songplays table
    #seachvisits_table = df.join(searches_df, df.visitor_id == searches_df.visitor_id, how='inner')\
    #                    .select(monotonically_increasing_id().alias("searchvisit_id"),col("start_time"),col("visits").alias("visits"),"hits_avg","registered","country", col("region").alias("region"), "first_hit_pagename", col("countPerday").alias("countPerday"),col("flight_date_outbound"))

    #seachvisits_table = seachvisits_table.join(time_table, seachvisits_table.start_time == time_table.start_time, how="inner")\
    #                    .select("searchvisit_id", seachvisits_tableseachvisits_table.start_time, "first_hit_pagename"  "year", "month")

    # write songplays table to parquet files partitioned by year and month
    #seachvisits_table.drop_duplicates().write.parquet(os.path.join(output_data, "searches-visit-out/"), mode="overwrite")

def process_valid_files(bucket_name,input_stage_file):
    
    """
    Description:
            Process the files to archive.
    :param file: file to archive
    """
    file_path="inbound/test/stage/"+input_stage_file+"/"
    out_file_path="inbound/test"+input_stage_file+"/"
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_path, StartAfter=file_path)
    #print(response["Contents"])
    
    file_archive_path="inbound/test/"+input_stage_file+"-archive-input/"
    response_archive = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_archive_path, StartAfter=file_archive_path)
    
    #print(response_archive["Contents"])
    archive_list=[]
    for archive_file in response_archive["Contents"]:
        archive_list.append(archive_file["Key"].split("/")[-1])

    for process_file in response["Contents"]:
        if process_file["Key"].split("/")[-1] not in archive_list:
            s3.copy_object(CopySource = bucket_name+'/'+file_path+process_file["Key"].split("/")[-1], Bucket = bucket_name, Key = out_file_path+process_file["Key"].split("/")[-1])
            s3.delete_object(Bucket=bucket_name, Key=process_file["Key"])

def process_archive_files(bucket_name,input_file):
    
    """
    Description:
            Process the files to archive.
    :param file: file to archive
    """
    file_path="inbound/test/"+input_file+"/"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_path, StartAfter=file_path)
    
    for process_file in response["Contents"]:
        archive_path_file= "inbound/test/"+input_file+"-archive-input/"+process_file["Key"].split("/")[-1]
        s3.copy_object(CopySource = bucket_name+'/'+file_path+process_file["Key"].split("/")[-1], Bucket = bucket_name, Key = archive_path_file)
        s3.delete_object(Bucket=bucket_name, Key=process_file["Key"])

def main():
    spark = create_spark_session()
    
    files=['searches','visitors']
    bucket_name="<<bucket>>"
    input_stage_data="s3://<<bucket>>/inbound/test/stage/" 
    input_data = "s3://<<bucket>>/inbound/test/"
    output_data = "s3://<<bucket>>/inbound/test/"
    
    for input_stage_file in files:
        process_valid_files(bucket_name,input_stage_file)
    
    process_searches_data(spark, input_data, output_data)
    process_visitors_data(spark, input_data, output_data)
    
    for input_file in files:
        process_archive_files(bucket_name,input_file)


if __name__ == "__main__":
    main()
