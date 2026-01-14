import os
import time
import json
import logging
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BROKER_HOST = os.getenv("BROKER_HOST", "rabbitmq")
BROKER_PORT = int(os.getenv("BROKER_PORT", 1883))
USER = os.getenv("RABBITMQ_USER", "user")
PASSWORD = os.getenv("RABBITMQ_PASS", "password")
TOPIC = os.getenv("TOPIC_NAME", "machine/data")

INFLUX_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
INFLUX_ORG = os.getenv("INFLUXDB_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "machine_data")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

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
        
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
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

    client.loop_forever()

if __name__ == "__main__":
    main()
