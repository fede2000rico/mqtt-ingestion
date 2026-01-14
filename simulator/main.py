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

def get_machine_data():
    """Generates random machine data simulating a production environment"""
    status_options = ["ON", "IDLE", "ALARM"]
    # higher probability for ON
    status = random.choices(status_options, weights=[0.7, 0.2, 0.1], k=1)[0]
    
    data = {
        "machine_id": "machine_01",
        "timestamp": time.time(),
        "status": status,
        "recipe_id": f"RECIPE_{random.randint(100, 105)}",
        "job_id": f"JOB_{random.randint(1000, 9999)}",
        "batch_id": f"BATCH_{random.randint(1, 50)}",
    }

    if status == "ALARM":
        data["alarm_code"] = f"E{random.randint(100, 999)}"
        data["alarm_message"] = "Unexpected sensor reading"
    else:
        # Some production metrics if running
        data["temperature"] = round(random.uniform(50.0, 80.0), 2)
        data["speed"] = round(random.uniform(100, 500), 1)

    return data

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

    try:
        while True:
            payload = get_machine_data()
            client.publish(TOPIC, json.dumps(payload))
            logging.info(f"Published: {payload}")
            time.sleep(2) # Send data every 2 seconds
    except KeyboardInterrupt:
        logging.info("Stopping simulator...")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
