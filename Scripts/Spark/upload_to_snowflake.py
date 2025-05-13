from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import os
import logging



# to find recent partation path 
def get_recent_partition_path(base_path, day_back=0):
    target_date = (datetime.now() - timedelta(days=day_back)).date()
    new_path = f"{base_path}/year={target_date.year}/month={target_date.month}/day={target_date.day}"
    return new_path


# get max timestamp from snow
def get_max_ts(spark, snow_options, table_name):
    try:
        # Create a query to select the max timestamp from the Snowflake view
        query = f"select * from silver.{table_name}"

        # Read from Snowflake using the provided options
        df = spark.read \
            .format("snowflake") \
            .options(**snow_options) \
            .option("query", query) \
            .load()

        # Collect the result to get the max timestamp
        max_ts_row = df.collect()[0]["MAX_TIMESTAMP"]
        
        logging.info(f"Max_ts for {table_name} is : {max_ts_row}")
        return max_ts_row

    except Exception as e:
        logging.error(f"Failed to fetch max timestamp: {e}")
        return None

    
# insert new row to snow 
def insert_to_snow(df,snow_op,table_name):
    try:
        df = df.drop("formatted_timestamp")
        if df.count() > 0:
           df.write \
          .format("snowflake") \
          .options(**snow_op) \
          .option("dbtable", table_name) \
          .mode("append") \
          .save()
        logging.info(f"Inserted {df.count()} rows into {table_name}")
    except Exception as e:
        logging.error(f"Failed to write data to Snowflake table {table_name}: {e}")



# build spark session
spark = SparkSession.builder \
    .appName("SnowflakeIntegration") \
    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.3") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

sms_dir = "hdfs://172.27.0.2:8020/silver/sms_cleaned"
call_dir = "hdfs://172.27.0.2:8020/silver/call_cleaned"


sms_path = get_recent_partition_path(sms_dir)
call_path = get_recent_partition_path(call_dir)

snow_options = {
        "sfURL": "JN79948.eu-central-2.aws.snowflakecomputing.com",
        "sfUser": "snowtest11",
        "sfPassword": "Sn1234567891011",
        "sfDatabase": "Telecom",
        "sfSchema": "SILVER",
        "sfWarehouse": "COMPUTE_WH",
        "sfRole": "ACCOUNTADMIN"
    }

sms_df = spark.read.parquet(sms_path)
call_df = spark.read.parquet(call_path)

sms_max_ts = get_max_ts(spark,snow_options,"cleaned_sms")
call_max_ts = get_max_ts(spark,snow_options,"cleaned_call")

if sms_max_ts:
    sms_df = sms_df.withColumn(
        "formatted_timestamp",
        F.to_timestamp(F.col("timestamp"), "dd-MM-yyyy HH:mm:ss")
    )
    sms_df = sms_df.filter(sms_df["formatted_timestamp"] > sms_max_ts)
    if sms_df.count() > 0:
        insert_to_snow(sms_df, snow_options, "SMS")

# Clean timestamp formatting and filter CALL_DATA
if call_max_ts:
    call_df = call_df.withColumn(
        "formatted_timestamp",
        F.to_timestamp(F.col("timestamp"), "dd-MM-yyyy HH:mm:ss")
    )
    call_df = call_df.filter(call_df["formatted_timestamp"]>call_max_ts)
    if call_df.count() > 0:
        insert_to_snow(call_df, snow_options, "CALL_DATA")
