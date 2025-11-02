"""
Stock Tick Ingestor (Simulator)

This script simulates a feed of stock ticks and publishes them
to a Google Cloud Pub/Sub topic.

It's intended to be run locally for development and testing.
"""
import time
import json
import random
import logging
from google.cloud import pubsub_v1

# --- Configuration ---
PROJECT_ID = "your-gcp-project-id" # CHANGE this
TOPIC_NAME = "stock-ticks"
SYMBOL = "NIFTY-50"
START_PRICE = 20000.0
# ---------------------

def publish_message(publisher, topic_path, message_data):
    """Publishes a single message to Pub/Sub."""
    try:
        data = json.dumps(message_data).encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.result() # Wait for publish to complete
        logging.info(f"Published: {message_data}")
    except Exception as e:
        logging.error(f"Error publishing message: {e}")

def generate_tick(current_price):
    """Generates a single, random stock tick."""
    # Generate a small price change
    price_change = random.uniform(-0.5, 0.5)
    new_price = round(current_price + price_change, 2)
    
    # Get current timestamp (Unix float)
    current_time_sec = time.time()
    
    return {
        "symbol": SYMBOL,
        "timestamp": current_time_sec, # e.g., 1678886400.123
        "last_price": new_price,
        "volume": random.randint(1, 100),
    }, new_price

def main():
    """Main loop to generate ticks every second."""
    logging.basicConfig(level=logging.INFO)
    
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
    except Exception as e:
        logging.error(f"Could not connect to Pub/Sub: {e}")
        logging.error("Is the Pub/Sub emulator running? (gcloud beta emulators pubsub start)")
        logging.error("Have you set PUBSUB_EMULATOR_HOST=localhost:8085 ?")
        return

    logging.info(f"Starting tick simulator for '{SYMBOL}'...")
    logging.info(f"Publishing to topic: {topic_path}")
    
    current_price = START_PRICE
    try:
        while True:
            tick, current_price = generate_tick(current_price)
            publish_message(publisher, topic_path, tick)
            time.sleep(1) # Send one tick per second
    except KeyboardInterrupt:
        logging.info("\nSimulator stopped.")

if __name__ == "__main__":
    main()