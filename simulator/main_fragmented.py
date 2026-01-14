import os
import time
import json
import random
import paho.mqtt.client as mqtt
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
BROKER_HOST = os.getenv("BROKER_HOST", "rabbitmq")
BROKER_PORT = int(os.getenv("BROKER_PORT", 1883))
USER = os.getenv("RABBITMQ_USER", "user")
PASSWORD = os.getenv("RABBITMQ_PASS", "password")
TOPIC = os.getenv("TOPIC_NAME", "machine/data")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to RabbitMQ Broker!")
    else:
        logging.error(f"Failed to connect, return code {rc}")

def main():
    client = mqtt.Client()
    client.username_pw_set(USER, PASSWORD)
    client.on_connect = on_connect

    logging.info(f"Connecting to broker at {BROKER_HOST}:{BROKER_PORT}...")
    
    while True:
        try:
            client.connect(BROKER_HOST, BROKER_PORT, 60)
            break
        except Exception as e:
            logging.error(f"Connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    client.loop_start()

    # Initial Context
    recipe_id = f"RECIPE_{random.randint(100, 105)}"
    job_id = f"JOB_{random.randint(1000, 9999)}"
    batch_id = f"BATCH_{random.randint(1, 50)}"
    status = "IDLE"
    machine_id = "machine_01"

    try:
        while True:
            # Update Context occasionally (every 10 iterations -> ~20 seconds)
            if random.random() < 0.1:
                logging.info("Updating Context...")
                # Send Recipe
                recipe_id = f"RECIPE_{random.randint(100, 105)}"
                client.publish(TOPIC, json.dumps({"machine_id": machine_id, "recipe_id": recipe_id}))
                time.sleep(0.5)
                
                # Send Job
                job_id = f"JOB_{random.randint(1000, 9999)}"
                client.publish(TOPIC, json.dumps({"machine_id": machine_id, "job_id": job_id}))
                time.sleep(0.5)
                
                # Send Batch
                batch_id = f"BATCH_{random.randint(1, 50)}"
                client.publish(TOPIC, json.dumps({"machine_id": machine_id, "batch_id": batch_id}))
                time.sleep(0.5)

            # Update Status occasionally
            if random.random() < 0.2:
                status = random.choice(["ON", "IDLE", "ALARM"])
                client.publish(TOPIC, json.dumps({"machine_id": machine_id, "status": status}))
                logging.info(f"Status changed to {status}")
                time.sleep(0.5)

            # Send Telemetry (Trigger for ingestion)
            if status == "ALARM":
                 # Alarm payload
                 payload = {
                     "machine_id": machine_id,
                     "alarm_code": f"E{random.randint(100, 999)}",
                     "alarm_message": "Sensor Failure"
                 }
                 client.publish(TOPIC, json.dumps(payload))
                 logging.info(f"Sent Alarm: {payload}")
            elif status == "ON":
                 # Production data
                 payload = {
                     "machine_id": machine_id,
                     "temperature": round(random.uniform(50.0, 80.0), 2),
                     "speed": round(random.uniform(100, 500), 1)
                 }
                 client.publish(TOPIC, json.dumps(payload))
                 logging.info(f"Sent Telemetry: {payload}")
            
            time.sleep(2)

    except KeyboardInterrupt:
        logging.info("Stopping simulator...")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
