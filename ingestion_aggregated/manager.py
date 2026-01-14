import time
import json
import logging
import os
from threading import Timer
from typing import Dict, List, Any, Optional
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from models import MachineDataCombined

class MachineState:
    def __init__(self, machine_id, expected_fields):
        self.machine_id = machine_id
        self.expected_fields = set(expected_fields)
        self.context: Dict[str, Any] = {}
        self.pending_data: List[Dict[str, Any]] = []
        self.timers: List[Timer] = []

    def update_context(self, key, value):
        self.context[key] = value

    def is_context_complete(self):
        # Check if all expected fields are in the current context
        return self.expected_fields.issubset(self.context.keys())

    def get_combined_data(self, data_payload):
        combined = self.context.copy()
        combined.update(data_payload)
        combined["machine_id"] = self.machine_id
        return combined

class IngestionManager:
    def __init__(self, config: Dict, db_backend):
        self.config = config
        self.db = db_backend
        self.machines: Dict[str, MachineState] = {}
        self.timeout_seconds = 5.0 # Wait up to 5s for context

        # Initialize Machines
        for m in config.get("machines", []):
            mid = m["machine_id"]
            self.machines[mid] = MachineState(mid, m.get("expected_messages", []))

    def process_message(self, topic, payload: dict):
        machine_id = payload.get("machine_id")
        if not machine_id:
            return

        if machine_id not in self.machines:
            # Auto-register or Ignore? Let's ignore for safety unless configured
            # But specific requirements said "from config", so we check config.
            # If not in config, maybe we should warn.
            logging.warning(f"Unknown machine_id: {machine_id}")
            return

        state = self.machines[machine_id]

        # Identify message type and update context
        # We assume keys correspond to config expected_messages
        # E.g. {"recipe_id": "..."} update context
        
        is_data = False
        
        # Simple heuristic: if it contains 'temperature' or 'speed' or 'alarm' -> Data
        # Else -> Context (if matches expected keys)
        
        data_keys = ["temperature", "speed", "alarm_code", "alarm_message"]
        if any(k in payload for k in data_keys):
            is_data = True
        
        # Update context
        for key, value in payload.items():
            if key in state.expected_fields:
                state.update_context(key, value)
                logging.debug(f"[{machine_id}] Context Updated: {key}={value}")

        if is_data:
            self._handle_data(state, payload)

    def _handle_data(self, state: MachineState, data_payload: dict):
        if state.is_context_complete():
            logging.info(f"[{state.machine_id}] Context Complete. Writing Data immediately.")
            self._write_to_influx(state, data_payload)
        else:
            logging.info(f"[{state.machine_id}] Context Partial. Buffering Data... (Waiting {self.timeout_seconds}s)")
            # Buffer data and set timeout
            # We use a closure or partial to capture the specific data instance
            state.pending_data.append(data_payload)
            
            t = Timer(self.timeout_seconds, self._flush_buffered_data, args=[state, data_payload])
            state.timers.append(t)
            t.start()

    def _flush_buffered_data(self, state: MachineState, data_payload: dict):
        # This is called after timeout.
        # We check if context is now complete (maybe it arrived while waiting).
        # Regardless, we write what we have (Partial or Full).
        
        # Remove from pending (logic simplified here)
        if data_payload in state.pending_data:
            state.pending_data.remove(data_payload)
            
        if state.is_context_complete():
            logging.info(f"[{state.machine_id}] Timeout Reached: Context NOW Complete. Writing.")
        else:
            logging.warning(f"[{state.machine_id}] Timeout Reached: Context STILL Missing {state.expected_fields - state.context.keys()}. Writing Partial.")

        self._write_to_influx(state, data_payload)

    def _write_to_influx(self, state: MachineState, data_payload: dict):
        try:
            # Combine Context + Data
            combined_dict = state.get_combined_data(data_payload)
            
            # Use Pydantic to validate/sanitize
            # (Note: Pydantic model here is permissive with Optionals, allows partials)
            model = MachineDataCombined(**combined_dict)
            
            point = Point("production_data").tag("machine_id", model.machine_id)
            
            # Tags (Context)
            if model.recipe_id: point.tag("recipe_id", model.recipe_id)
            if model.job_id: point.tag("job_id", model.job_id)
            if model.batch_id: point.tag("batch_id", model.batch_id)
            if model.status: point.tag("status", model.status)
            
            # Fields (Data)
            if model.temperature is not None: point.field("temperature", model.temperature)
            if model.speed is not None: point.field("speed", model.speed)
            if model.alarm_code: 
                point.field("alarm_code", model.alarm_code)
                if model.alarm_message: point.field("alarm_message", model.alarm_message)

            self.db.write_point(point)
            logging.info(f"[{state.machine_id}] WROTE: {model.model_dump(exclude_none=True)}")
            
        except Exception as e:
            logging.error(f"Write Error: {e}")

    def close(self):
        for m in self.machines.values():
            for t in m.timers:
                t.cancel()
        self.db.close()
