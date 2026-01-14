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

# Global Context Storage
# Structure: { "machine_01": { "recipe_id": "...", "job_id": "...", "status": "ON", ... } }
machine_contexts = {}

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
        machine_id = payload.get("machine_id")

        if not machine_id:
            logging.warning("Received message without machine_id, ignoring.")
            return

        # Initialize context for this machine if not exists
        if machine_id not in machine_contexts:
            machine_contexts[machine_id] = {}

        # Update context with known keys
        context_keys = ["recipe_id", "job_id", "batch_id", "status"]
        updated_context = False
        for key in context_keys:
            if key in payload:
                machine_contexts[machine_id][key] = payload[key]
                updated_context = True
                logging.info(f"Updated context for {machine_id}: {key} = {payload[key]}")

        # Check if this is a "Trigger" message (Telemetry or Alarm)
        is_telemetry = "temperature" in payload or "speed" in payload
        is_alarm = "alarm_code" in payload

        if is_telemetry or is_alarm:
            # We need to build the full point now
            current_context = machine_contexts[machine_id]
            
            # Basic validation: ensure we have at least some context if needed
            # For now, we allow partial context, or we could enforce "status" presence
            status = current_context.get("status", "UNKNOWN")
            
            point = Point("production_data") \
                .tag("machine_id", machine_id) \
                .tag("status", status) \
                .tag("recipe_id", current_context.get("recipe_id", "N/A")) \
                .tag("job_id", current_context.get("job_id", "N/A")) \
                .tag("batch_id", current_context.get("batch_id", "N/A"))
            
            if is_telemetry:
                if "temperature" in payload:
                    point = point.field("temperature", float(payload["temperature"]))
                if "speed" in payload:
                    point = point.field("speed", float(payload["speed"]))
            
            if is_alarm:
                point = point.field("alarm_code", payload["alarm_code"])
                if "alarm_message" in payload:
                    point = point.field("alarm_message", payload["alarm_message"])

            # Write to InfluxDB
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            logging.info(f"Data written to InfluxDB for {machine_id} (Context: {current_context.get('job_id')})")
            
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
