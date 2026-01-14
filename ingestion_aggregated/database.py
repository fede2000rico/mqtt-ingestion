import os
import logging
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class InfluxBackend:
    def __init__(self):
        self.url = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
        self.token = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
        self.org = os.getenv("INFLUXDB_ORG", "my-org")
        self.bucket = os.getenv("INFLUXDB_BUCKET", "machine_data")
        
        try:
            self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            logging.info("InfluxDB Client Initialized")
        except Exception as e:
            logging.error(f"Failed to init InfluxDB: {e}")
            raise e

    def write_point(self, point: Point):
        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
        except Exception as e:
            logging.error(f"InfluxDB Write Error: {e}")
            raise e

    def close(self):
        self.client.close()
