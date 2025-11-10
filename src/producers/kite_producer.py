"""
Kite Connect (Real) Producer

This script connects to the Kite Connect WebSocket API to receive
live market ticks and publishes them to a Google Cloud Pub/Sub topic.
"""
import logging
import json
import os
from google.cloud import pubsub_v1
from kiteconnect import KiteTicker

# --- Configuration ---
# These will be set as environment variables
API_KEY = os.environ.get('KITE_API_KEY')
API_SECRET = os.environ.get('KITE_API_SECRET')
ACCESS_TOKEN = os.environ.get('KITE_ACCESS_TOKEN')
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
TOPIC_NAME = 'stock-ticks'

# --- Instrument IDs to subscribe to (e.g., NIFTY 50 index) ---
# You would fetch these dynamically or configure them
INSTRUMENT_IDS = [256265] # Example: NIFTY 50

# --- Global Clients (for reuse) ---
try:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
except Exception as e:
    logging.critical(f"Failed to initialize Pub/Sub client: {e}")
    publisher = None

# --- WebSocket Event Handlers ---

def on_ticks(ws, ticks):
    """
    Callback for when a new tick is received.
    """
    if not publisher:
        logging.warning("Pub/Sub client not available. Skipping tick.")
        return
        
    for tick in ticks:
        try:
            # Prepare data for Pub/Sub
            # We want to match the simulator's format
            message_data = {
                "symbol": tick['instrument_token'], # You'll map this to a symbol later
                "timestamp": tick['timestamp'].timestamp(), # Convert datetime to Unix float
                "last_price": tick['last_price'],
                "volume": tick['volume_traded'], # Use volume_traded for the tick
            }
            
            data_bytes = json.dumps(message_data).encode("utf-8")
            
            # Publish message to Pub/Sub
            future = publisher.publish(topic_path, data_bytes)
            future.result(timeout=10) # Block until published
            
            logging.info(f"Published tick: {message_data}")
            
        except Exception as e:
            logging.error(f"Error processing tick {tick}: {e}")

def on_connect(ws, response):
    """
    Callback on successful WebSocket connection.
    Subscribes to the specified instruments.
    """
    logging.info("WebSocket connected. Subscribing to instruments...")
    ws.subscribe(INSTRUMENT_IDS)
    ws.set_mode(ws.MODE_FULL, INSTRUMENT_IDS) # Get full tick data

def on_close(ws, code, reason):
    """
Callback when WebSocket connection is closed.
"""
    logging.warning(f"WebSocket closed: {code} - {reason}")

def on_error(ws, code, reason):
    """Callback for WebSocket errors."""
    logging.error(f"WebSocket error: {code} - {reason}")

def on_reconnect(ws, attempts_count):
    """Callback when attempting to reconnect."""
    logging.warning(f"Attempting to reconnect... (Attempt {attempts_count})")

# --- Main Entry Point ---

def main():
    """
    Main function to start the KiteTicker client.
    This is what your Cloud Function will call.
    """
    logging.basicConfig(level=logging.INFO)
    
    if not all([API_KEY, API_SECRET, ACCESS_TOKEN, PROJECT_ID, publisher]):
        logging.critical("Missing critical environment variables! Exiting.")
        return

    logging.info("Starting Kite Connect producer...")
    
    # Initialize the KiteTicker
    kws = KiteTicker(API_KEY, ACCESS_TOKEN)
    
    # Assign callbacks
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_reconnect = on_reconnect
    
    # Start the WebSocket connection
    # This is a blocking call, it will run forever
    kws.connect(threaded=False) 

if __name__ == "__main__":
    # This allows you to run it locally as a script
    main()