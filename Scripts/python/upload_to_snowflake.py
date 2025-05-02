from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool
import os
import logging
from hdfs import InsecureClient
from dotenv import load_dotenv
import pandas as pd
import shutil
import pyarrow.parquet as pq
from snowflake.connector.errors import DatabaseError


def snowflake_connection(user, password, account, warehouse, database, schema, role):
    conn_str = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}"
    try:
        engine = create_engine(conn_str, poolclass=NullPool)
        print("Connected to Snowflake successfully")
        return engine
    except SQLAlchemyError as e:
        print(f"Error while connecting to Snowflake: {e}")
        return None


def insert_raw_data(table_name, data_frame, connection, engine):
    logging.info(f"Loading raw data into table {table_name}")

    if connection is not None:
        try:
            data_frame.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            logging.info(f"Raw data loaded successfully into {table_name} with {len(data_frame)} rows")
        except SQLAlchemyError as e:
            logging.error(f"Error while loading raw data into {table_name}: {e}")
    else:
        logging.error(f"Failed to connect to the database. Cannot load data into table {table_name}")


def read_parquet_from_hdfs(hdfs_uri, hdfs_path):
    client = InsecureClient(hdfs_uri, user='hadoop')

    local_root = "/opt/airflow/includes"
    local_dir_name = os.path.basename(hdfs_path.rstrip('/'))
    local_path = os.path.join(local_root, local_dir_name)

    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    os.makedirs(local_path, exist_ok=True)

    logging.info(f"Downloading from HDFS: {hdfs_path} to {local_path}")
    client.download(hdfs_path, local_path)

    dfs = []
    parquet_files = []
    for root, dirs, files in os.walk(local_path):
        for file in files:
            if file.endswith('.parquet'):
                parquet_files.append(os.path.join(root, file))

    if not parquet_files:
        logging.warning(f"No .parquet files found in {local_path}")

    for parquet_file in parquet_files:
        logging.info(f"Reading {parquet_file}")
        table = pq.read_table(parquet_file)
        df = table.to_pandas()
        dfs.append(df)

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()


if __name__ == "__main__":
    load_dotenv("environment.env")

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("snowflake_uploader")

    # Snowflake credentials
    SNOWFLAKE_USER = "snowtest11"
    SNOWFLAKE_PASSWORD = "Sn1234567891011"
    SNOWFLAKE_ROLE = "ACCOUNTADMIN"
    SNOWFLAKE_ACCOUNT = "JN79948.eu-central-2.aws"
    SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
    SNOWFLAKE_DATABASE = "Telecom"
    SNOWFLAKE_SCHEMA = "SILVER"

    # HDFS
    HDFS_URI = "http://namenode:9870"
    sms_dir = "/silver/sms_cleaned"
    call_dir = "/silver/call_cleaned"

    # Connect to Snowflake
    engine = snowflake_connection(
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_WAREHOUSE,
        SNOWFLAKE_DATABASE,
        SNOWFLAKE_SCHEMA,
        SNOWFLAKE_ROLE
    )

    if engine:
        try:
            sms_df = read_parquet_from_hdfs(HDFS_URI, sms_dir)
            call_df = read_parquet_from_hdfs(HDFS_URI, call_dir)

            with engine.connect() as connection:
                if not sms_df.empty:
                    insert_raw_data("SMS", sms_df, connection, engine)
                else:
                    logging.warning("SMS DataFrame is empty. Skipping insert.")

                if not call_df.empty:
<<<<<<< HEAD
                    insert_raw_data("CALL_DATA", call_df, connection, engine)
=======
                    insert_raw_data("Call", call_df, connection, engine)
>>>>>>> 2872f63 (init reop)
                else:
                    logging.warning("Call DataFrame is empty. Skipping insert.")

        except DatabaseError as e:
            logging.error(f"Snowflake Database error: {e}")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
