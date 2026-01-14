import os
import time
import paho.mqtt.client as mqtt

# Configuration
BROKER_HOST = os.getenv("BROKER_HOST", "localhost")
BROKER_PORT = int(os.getenv("BROKER_PORT", 1883))
USER = os.getenv("RABBITMQ_USER", "user")
PASSWORD = os.getenv("RABBITMQ_PASS", "password")

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    # Subscribe to everything
    client.subscribe("#")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
    except:
        payload = msg.payload
    print(f"[{msg.topic}] {payload}")

client = mqtt.Client()
client.username_pw_set(USER, PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

print(f"Connecting to {BROKER_HOST}:{BROKER_PORT}...")
try:
    client.connect(BROKER_HOST, BROKER_PORT, 60)
    client.loop_forever()
except Exception as e:
    print(f"Connection failed: {e}")
