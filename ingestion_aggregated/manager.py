import time
import json
import logging
import os
from threading import Timer
from typing import Dict, List, Any, Optional
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from models import MachineDataCombined

class MachineBuffer:
    def __init__(self, machine_id, expected_messages):
        self.machine_id = machine_id
        # We need to map expected messages to internal queues
        # e.g. "recipe_id" -> buffer["recipe_id"] = []
        self.queues: Dict[str, List[Any]] = {msg_type: [] for msg_type in expected_messages}
        # Data/Status/Etc are treated equally here since config defines expected fields.
        
        # Max buffer size to avoid memory leak if unaligned
        self.MAX_SIZE = 100

    def add_message(self, msg_type, payload):
        if msg_type in self.queues:
            self.queues[msg_type].append(payload)
            # Simple overflow protection
            if len(self.queues[msg_type]) > self.MAX_SIZE:
                 self.queues[msg_type].pop(0)
                 logging.warning(f"[{self.machine_id}] Queue '{msg_type}' overflow, dropped oldest.")

    def is_row_ready(self):
        # Ready if ALL queues have at least 1 item
        return all(len(q) > 0 for q in self.queues.values())

    def pop_row(self):
        if not self.is_row_ready():
            return None
        
        row_data = {}
        for msg_type in self.queues:
            # Pop the oldest
            item = self.queues[msg_type].pop(0)
            row_data.update(item)
        
        row_data["machine_id"] = self.machine_id
        return row_data

class IngestionManager:
    def __init__(self, config: Dict):
        self.config = config
        self.machines: Dict[str, MachineBuffer] = {}
        
        # InfluxDB Init
        self.url = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
        self.token = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
        self.org = os.getenv("INFLUXDB_ORG", "my-org")
        self.bucket = os.getenv("INFLUXDB_BUCKET", "machine_data")
        
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            logging.info("InfluxDB Client Initialized (Buffered Mode)")
        except Exception as e:
            logging.error(f"Failed to init InfluxDB: {e}")

        # Initialize Machines
        for m in config.get("machines", []):
            mid = m["machine_id"]
            # "expected_messages" keys are what we queue
            self.machines[mid] = MachineBuffer(mid, m.get("expected_messages", []))

    def process_message(self, topic, payload: dict):
        machine_id = payload.get("machine_id")
        if not machine_id or machine_id not in self.machines:
            return

        buffer = self.machines[machine_id]
        
        # Identify "Type" of message to route to correct queue
        # Heuristic: Check which key from expected_messages is present'
        # Note: If a message contains multiple keys, we might queue it in multiple queues or just one?
        # User implies separated messages: "recipe", "job", "batch".
        
        # Special case: "telemetry" usually contains temperature/speed.
        # But config says: ["recipe_id", "job_id", "batch_id", "status"] in previous example.
        # If config was ["recipe_id", "temperature"], we look for those keys.
        
        enqueued = False
        for key in buffer.queues.keys():
            # If payload has this key (and maybe others), we treat it as that type
            # Problem: if payload has {recipe_id: 1, job_id: 2}, do we add to both?
            # User said "queue for EACH data type".
            # I will assume "Fragmented" input: only one key per message.
            if key in payload:
                buffer.add_message(key, payload)
                logging.debug(f"[{machine_id}] Enqueued {key}")
                enqueued = True

        # Handle "Data" / "Telemetry" implicitly if not explicitly in proper keys?
        # If user config has "temperature", we queue it.
        # If the user configuration does NOT list 'temperature' but explicitly lists 'recipe',
        # and we receive 'temperature', we drop it?
        # The user instructions were vague on config structure for "buffered".
        # I will assume the config lists ALL keys we want to sync.
        
        if enqueued:
            self._check_and_flush(buffer)

    def _check_and_flush(self, buffer: MachineBuffer):
        # Loop while we have complete rows
        while buffer.is_row_ready():
            row = buffer.pop_row()
            if row:
                self._write_to_influx(row)

    def _write_to_influx(self, combined_dict: dict):
        try:
            # Use Pydantic to validate
            model = MachineDataCombined(**combined_dict)
            
            point = Point("production_data").tag("machine_id", model.machine_id)
            
            if model.recipe_id: point.tag("recipe_id", model.recipe_id)
            if model.job_id: point.tag("job_id", model.job_id)
            if model.batch_id: point.tag("batch_id", model.batch_id)
            if model.status: point.tag("status", model.status)
            
            if model.temperature is not None: point.field("temperature", model.temperature)
            if model.speed is not None: point.field("speed", model.speed)
            if model.alarm_code: point.field("alarm_code", model.alarm_code)

            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            logging.info(f"[{model.machine_id}] WROTE Buffered Row: {model.model_dump(exclude_none=True)}")
            
        except Exception as e:
            logging.error(f"Write Error: {e}")

    def close(self):
        self.client.close()
