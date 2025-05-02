from kafka import KafkaProducer
import json
import time
import logging
from telcom_data_simulator import main as generate_events
<<<<<<< HEAD
import random
=======
>>>>>>> 2872f63 (init reop)
import os



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('producer')


TOPIC = ["sms_event", "call_event"]
BOOTSTRAP_SERVERS ="kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def stream_events(num_events):
    """
    Stream events to Kafka topic.
    """
    logger.info(f"Streaming {num_events} events to Kafka topic '{TOPIC}'")
    all_events = generate_events(num_events)
    
    for event in all_events:
        event_type = event.get("event_type")
        if event_type == "sms":
            topic = TOPIC[0]
        elif event_type == "call":
            topic = TOPIC[1]
        else:
            logger.error(f"Unknown event type: {event_type}")
            continue
        producer.send(topic, value=event)
<<<<<<< HEAD
        logger.info(f"Sent event: ")
        time.sleep(random.uniform(0.1, 3.0))
=======
        logger.info(f"Sent event: {event}")
        time.sleep(0.5)  
>>>>>>> 2872f63 (init reop)

    producer.flush()
    logger.info("All events sent successfully.")

if __name__ == "__main__":
    try:
        logger.info("Starting event streaming...")
<<<<<<< HEAD
        stream_events(200)
=======
        stream_events(5)
>>>>>>> 2872f63 (init reop)
        logger.info("Finished streaming events.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")