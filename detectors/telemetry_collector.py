import time
import json
import os
import sys
from datetime import datetime
from confluent_kafka import Consumer, TopicPartition
import psutil

# Add the parent directory to the path so we can import from database.py
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from simulator.database import get_connection

class TelemetryCollector:
    def __init__(self, bootstrap_servers="localhost:9092", interval_sec=5):
        self.bootstrap_servers = bootstrap_servers
        self.interval_sec = interval_sec
        self.log_file = "detectors/telemetry_log.jsonl"
        os.makedirs("detectors", exist_ok=True)

    def get_consumer_lag(self, topic="ehr-stream", group_id="connect-databricks-sink"):
        """Calculates the exact consumer lag for the Kafka Connect sink."""
        try:
            # We create a temporary consumer just to inspect offsets
            c = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': group_id,
                'enable.auto.commit': False
            })
            
            # 1. Get partition metadata for the topic
            metadata = c.list_topics(topic=topic, timeout=5.0)
            if topic not in metadata.topics:
                return 0
                
            partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]
            
            # 2. Get the currently committed offsets for the sink connector
            committed = c.committed(partitions, timeout=5.0)
            
            total_lag = 0
            for tp in committed:
                # 3. Get the absolute latest offset (High Watermark)
                low, high = c.get_watermark_offsets(tp)
                
                if tp.offset >= 0:  # If the connector has committed an offset
                    lag = high - tp.offset
                    total_lag += lag
                elif high > 0:      # If it has never committed, lag is the total messages
                    total_lag += high
                    
            c.close()
            return total_lag
        except Exception as e:
            print(f"[TELEMETRY ERROR] Lag calculation failed: {e}")
            return 0

    def get_databricks_latency(self):
        """Measures warehouse responsiveness via a lightweight ping query."""
        start_time = time.time()
        try:
            con = get_connection()
            cursor = con.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            con.close()
            latency = time.time() - start_time
            return round(latency, 4)
        except Exception as e:
            print(f"[TELEMETRY ERROR] Databricks ping failed: {e}")
            return 99.9  # Return a massive spike if unreachable

    def get_system_metrics(self):
        """Grabs local CPU and Memory to proxy Kafka broker health."""
        return {
            "cpu_percent": psutil.cpu_percent(interval=None),
            "memory_percent": psutil.virtual_memory().percent
        }

    def collect(self):
        """Runs a single collection cycle and writes to the telemetry log."""
        lag = self.get_consumer_lag()
        db_latency = self.get_databricks_latency()
        sys_metrics = self.get_system_metrics()

        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "consumer_lag": lag,
            "db_latency_sec": db_latency,
            "cpu_percent": sys_metrics["cpu_percent"],
            "memory_percent": sys_metrics["memory_percent"]
        }

        # Write to an append-only JSON lines file for the detectors to read
        with open(self.log_file, "a") as f:
            f.write(json.dumps(payload) + "\n")

        print(f"[TELEMETRY] Lag: {lag:4} | DB Latency: {db_latency:.3f}s | CPU: {sys_metrics['cpu_percent']:>4.1f}%")
        return payload

    def start_polling(self):
        print(f"=== Starting Telemetry Collector (Interval: {self.interval_sec}s) ===")
        while True:
            self.collect()
            time.sleep(self.interval_sec)

if __name__ == "__main__":
    collector = TelemetryCollector()
    collector.start_polling()