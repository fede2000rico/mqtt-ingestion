import os
import time
import json
import logging
import paho.mqtt.client as mqtt
from influxdb_client import Point
from database import InfluxBackend

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BROKER_HOST = os.getenv("BROKER_HOST", "rabbitmq")
BROKER_PORT = int(os.getenv("BROKER_PORT", 1883))
USER = os.getenv("RABBITMQ_USER", "user")
PASSWORD = os.getenv("RABBITMQ_PASS", "password")
TOPIC = os.getenv("TOPIC_NAME", "machine/data")

# Init DB
try:
    db = InfluxBackend()
except Exception:
    exit(1)

def on_connect(client, userdata, flags, rc):
    logging.info(f"Connected to RabbitMQ Broker! Code: {rc}")
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        logging.info(f"Received: {payload}")
        
        point = Point("production_data")
        for k, v in payload.items():
            if isinstance(v, (int, float)):
                point = point.field(k, v)
            else:
                point = point.tag(k, str(v))
        
        db.write_point(point)
        logging.info("Written to InfluxDB")
            
    except Exception as e:
        logging.error(f"Error: {e}")

def main():
    client = mqtt.Client()
    client.username_pw_set(USER, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    
    while True:
        try:
            client.connect(BROKER_HOST, BROKER_PORT, 60)
            break
        except:
            time.sleep(5)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        db.close()
        client.disconnect()

if __name__ == "__main__":
    main()
