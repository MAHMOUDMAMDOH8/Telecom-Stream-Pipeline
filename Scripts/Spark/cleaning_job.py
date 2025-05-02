from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('spark_cleaning_job')

spark = SparkSession.builder \
    .appName("spark_cleaning_job") \
    .master("spark://172.27.0.8:7077") \
    .getOrCreate()

hdfs_rpc_prefix = "hdfs://172.27.0.4:8020"
hdfs_bronze_path = "/bronze"
hdfs_silver_path = "/silver"

csv_path = f"{hdfs_rpc_prefix}{hdfs_bronze_path}/includes.csv"
csv_df = spark.read.csv(csv_path, header=True)
file_name_list = csv_df.select("file_name").rdd.flatMap(lambda x: x).collect()



for file_name in file_name_list:

    if file_name.startswith("sms"):
        path = f"{hdfs_rpc_prefix}{hdfs_bronze_path}/{file_name}"
        dest_path = f"{hdfs_rpc_prefix}{hdfs_silver_path}/sms_cleaned"
        logger.info(f"Processing file: {path}")

        sms_schema = StructType([
                StructField("event_type", StringType(), True),
                StructField("sid", StringType(), True),
                StructField("from", StructType([
                    StructField("cell_site", StringType(), True),
                    StructField("customer_id", LongType(), True),
                    StructField("imei", StringType(), True),
                    StructField("number", StringType(), True),
                    StructField("plan_type", StringType(), True)
                ]), True),
                StructField("to", StructType([
                    StructField("cell_site", StringType(), True),
                    StructField("customer_id", LongType(), True),
                    StructField("imei", StringType(), True),
                    StructField("number", StringType(), True),
                    StructField("plan_type", StringType(), True)
                ]), True),
                StructField("body", StringType(), True),
                StructField("status", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("customer_id", LongType(), True),
                StructField("registration_date", StringType(), True),
                StructField("billing_info", StructType([
                    StructField("amount", DoubleType(), True),
                    StructField("currency", StringType(), True)
                ]), True)
        ])
        sms_schema.add(StructField("_corrupt_record", StringType(), True))
        df = spark.read.option("multiLine", "true") \
                           .option("mode", "PERMISSIVE") \
                           .option("columnNameOfCorruptRecord", "_corrupt_record") \
                           .schema(sms_schema) \
                           .json(path)

        result_df = df.select(
            "event_type",
            "sid",
            df["from.number"].alias("sender"),
            df["from.imei"].alias("imei_sender"),
            df["from.plan_type"].alias("sender_plan_type"),
            df["from.cell_site"].alias("sender_cell_site"),
            df["to.number"].alias("receiver"),
            df["to.imei"].alias("imei_receiver"),
            df["to.plan_type"].alias("receiver_plan_type"),
            df["to.cell_site"].alias("receiver_cell_site"),
            "body",
            "status",
            "timestamp",
            "registration_date",
            df["billing_info.amount"].alias("amount"),
            df["billing_info.currency"].alias("currency")
        )
        result_df.coalesce(1).write.mode("overwrite").parquet(dest_path)
    
    elif file_name.startswith("call"):
        path = f"{hdfs_rpc_prefix}{hdfs_bronze_path}/{file_name}"
        dest_path = f"{hdfs_rpc_prefix}{hdfs_silver_path}/call_cleaned"
        logger.info(f"Processing file: {path}")

        call_schema = StructType([
                StructField("event_type", StringType(), True),
                StructField("sid", StringType(), True),
                StructField("from", StructType([
                    StructField("number", StringType(), True),
                    StructField("cell_site", StringType(), True),
                    StructField("imei", StringType(), True),
                    StructField("customer_id", LongType(), True),
                    StructField("plan_type", StringType(), True)
                ]), True),
                StructField("to", StructType([
                    StructField("number", StringType(), True),
                    StructField("cell_site", StringType(), True),
                    StructField("imei", StringType(), True),
                    StructField("customer_id", LongType(), True),
                    StructField("plan_type", StringType(), True)
                ]), True),
                StructField("call_duration_seconds", DoubleType(), True),
                StructField("status", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("call_type", StringType(), True),
                StructField("billing_info", StructType([
                    StructField("amount", DoubleType(), True),
                    StructField("currency", StringType(), True)
                ]), True)
        ])
        call_schema.add(StructField("_corrupt_record", StringType(), True))

        call_df = spark.read.option("multiLine", "true") \
                           .option("mode", "PERMISSIVE") \
                           .option("columnNameOfCorruptRecord", "_corrupt_record") \
                           .schema(call_schema) \
                           .json(path)

        result_df = call_df.select(
            "event_type",
            "sid",
            call_df["from.number"].alias("caller"),
            call_df["from.imei"].alias("device_caller"),
            call_df["from.plan_type"].alias("caller_plan_type"),
            call_df["from.cell_site"].alias("caller_cell_site"),
            call_df["to.number"].alias("receiver"),
            call_df["to.imei"].alias("device_receiver"),
            call_df["to.plan_type"].alias("receiver_plan_type"),
            call_df["to.cell_site"].alias("receiver_cell_site"),
            "call_duration_seconds",
            "call_type",
            "status",
            "timestamp",
            call_df["billing_info.amount"].alias("amount"),
            call_df["billing_info.currency"].alias("currency")
        )

        result_df.coalesce(1).write.mode("overwrite").parquet(dest_path)



