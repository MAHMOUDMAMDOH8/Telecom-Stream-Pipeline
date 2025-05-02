from kafka import KafkaConsumer
import json
import logging
import os
from hdfs import InsecureClient
from datetime import datetime
from time import time
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consumer')

# --- HDFS Config ---
HDFS_URI = "http://namenode:9870"
HDFS_DIR = "/bronze"
client = InsecureClient(HDFS_URI, user="hdfs")

# --- HDFS Append Helper ---
def append_event_to_hdfs(event, file_name):
    try:
        file_path = os.path.join(HDFS_DIR, file_name)
        # add files name to csv in the same directory
        
        
        # Read existing file if exists
        if client.status(file_path, strict=False):
            with client.read(file_path, encoding='utf-8') as reader:
                try:
                    existing_data = json.load(reader)
                    if not isinstance(existing_data, list):
                        existing_data = [existing_data]
                except json.JSONDecodeError:
                    existing_data = []
        else:
            existing_data = []

        # Append event
        existing_data.append(event)

        # Overwrite file with new data
        with client.write(file_path, encoding='utf-8', overwrite=True) as writer:
            json.dump(existing_data, writer, ensure_ascii=False, indent=2)


        logger.info(f"Appended event to hdfs")

    except Exception as e:
        logger.error(f"Failed to write to {file_name}: {e}")

def upload_file_to_hdfs(file_path):
    try:
        hdfs_path = os.path.join(HDFS_DIR, os.path.basename(file_path))
        client.upload(hdfs_path, file_path, overwrite=True)
        logger.info(f"Uploaded {file_path} to HDFS at {hdfs_path}")
    except Exception as e:
        logger.error(f"Failed to upload {file_path} to HDFS: {e}")

# --- Kafka Setup ---
TOPICS = ["sms_event", "call_event"]
BOOTSTRAP_SERVERS = "kafka:9092"

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="telecom-consumer-group"
)

# --- Event Consumption ---
def consume_events():
    logger.info("🚀 Starting to consume events...")

    session_time = datetime.now().strftime('%Y%m%d_%H%M')
    sms_file_name = f"sms_events_{session_time}.json"
    call_file_name = f"call_events_{session_time}.json"
    list_name = [sms_file_name, call_file_name]
    df = pd.DataFrame(list_name, columns=['file_name'])
    df.to_csv('/opt/airflow/includes.csv', index=False)

    timeout_seconds = 10 
    last_msg_time = time()

    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)
            if not msg_pack:
                if time() - last_msg_time > timeout_seconds:
                    logger.info("No messages received for a while. Exiting.")
                    break
                continue

            counter = 0

            for tp, messages in msg_pack.items():
                for message in messages:
                    event = message.value
                    topic = message.topic
                    last_msg_time = time()

                    if topic == TOPICS[0]:
                        append_event_to_hdfs(event, sms_file_name)

                        counter += 1
                    elif topic == TOPICS[1]:
                        append_event_to_hdfs(event, call_file_name)
                        counter += 1
                    logger.info(f"counter {counter}")

    except KeyboardInterrupt:
        logger.warning("Consumer stopped manually.")
    
    upload_file_to_hdfs('/opt/airflow/includes.csv')

    consumer.close()
    logger.info("Consumer closed.")
    logger.info("Finished consuming events.")


# --- Run Consumer ---
if __name__ == "__main__":
    consume_events()
