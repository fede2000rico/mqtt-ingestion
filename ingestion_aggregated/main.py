import os
import time
import json
import logging
import paho.mqtt.client as mqtt
from manager import IngestionManager
from database import InfluxBackend

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
BROKER_HOST = os.getenv("BROKER_HOST", "rabbitmq")
BROKER_PORT = int(os.getenv("BROKER_PORT", 1883))
USER = os.getenv("RABBITMQ_USER", "user")
PASSWORD = os.getenv("RABBITMQ_PASS", "password")
TOPIC = os.getenv("TOPIC_NAME", "machine/data")
# Config mapped from volume
CONFIG_PATH = "/app/machines_config.json" 

# Load Config
if not os.path.exists(CONFIG_PATH):
    logging.error(f"Config file not found at {CONFIG_PATH}. Make sure to mount it.")
    exit(1)

try:
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
        logging.info("Machines Configuration loaded.")
except Exception as e:
    logging.error(f"Failed to load config: {e}")
    exit(1)

# Initialize Components
try:
    db_backend = InfluxBackend()
    manager = IngestionManager(config, db_backend)
except Exception as e:
    logging.error(f"Failed to initialize components: {e}")
    exit(1)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to RabbitMQ Broker!")
        client.subscribe(TOPIC)
        logging.info(f"Subscribed to topic: {TOPIC}")
    else:
        logging.error(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        manager.process_message(msg.topic, payload)
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON payload")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def main():
    client = mqtt.Client()
    client.username_pw_set(USER, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    logging.info(f"Connecting to broker at {BROKER_HOST}:{BROKER_PORT}...")
    
    while True:
        try:
            client.connect(BROKER_HOST, BROKER_PORT, 60)
            break
        except Exception as e:
            logging.error(f"Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logging.info("Stopping ingestion service...")
        manager.close()
        client.disconnect()

if __name__ == "__main__":
    main()
