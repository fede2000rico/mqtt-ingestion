import os
import time
import json
import logging
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
BROKER_HOST = os.getenv("BROKER_HOST", "rabbitmq")
BROKER_PORT = int(os.getenv("BROKER_PORT", 1883))
USER = os.getenv("RABBITMQ_USER", "user")
PASSWORD = os.getenv("RABBITMQ_PASS", "password")
TOPIC = os.getenv("TOPIC_NAME", "machine/data")

INFLUX_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
INFLUX_ORG = os.getenv("INFLUXDB_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "machine_data")

# InfluxDB Client
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

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
        logging.info(f"Received: {payload}")
        
        # Create InfluxDB Point
        point = Point("production_data") \
            .tag("machine_id", payload.get("machine_id")) \
            .tag("status", payload.get("status")) \
            .tag("recipe_id", payload.get("recipe_id")) \
            .tag("job_id", payload.get("job_id")) \
            .tag("batch_id", payload.get("batch_id"))
            
        # Add fields
        if "temperature" in payload:
            point = point.field("temperature", float(payload["temperature"]))
        if "speed" in payload:
            point = point.field("speed", float(payload["speed"]))
        if "alarm_code" in payload:
            point = point.field("alarm_code", payload["alarm_code"])
            point = point.field("alarm_message", payload["alarm_message"])
            
        # Write to InfluxDB
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logging.info("Data written to InfluxDB")
            
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

    client.loop_forever()

if __name__ == "__main__":
    main()
