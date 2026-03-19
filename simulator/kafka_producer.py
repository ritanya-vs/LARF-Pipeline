import json
import time
import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from confluent_kafka import Producer
from patient_generator import generate_patient_event

TOPICS = ["ehr-stream", "iot-vitals", "genomic-data"]

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")

def stream_events(duration_seconds=60, interval=1.0):
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print(f"[INFO] Streaming to Kafka for {duration_seconds}s (interval={interval}s)...")
    start = time.time()
    count = 0

    while time.time() - start < duration_seconds:
        event = generate_patient_event()

        # Primary topic gets all events
        producer.produce(
            "ehr-stream",
            key=event["patient_id"],
            value=json.dumps(event),
            callback=delivery_report
        )

        # iot-vitals gets just the sensor readings
        iot_payload = {
            "patient_id":    event["patient_id"],
            "heart_rate":    event["heart_rate"],
            "spo2":          event["spo2"],
            "timestamp":     event["timestamp"],
        }
        producer.produce(
            "iot-vitals",
            key=event["patient_id"],
            value=json.dumps(iot_payload),
            callback=delivery_report
        )

        producer.poll(0)
        count += 1

        if count % 10 == 0:
            elapsed = round(time.time() - start, 1)
            print(f"[INFO] {count} events sent ({elapsed}s elapsed)...")

        time.sleep(interval)

    producer.flush()
    print(f"[DONE] Streamed {count} events.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()
    stream_events(args.duration, args.interval)