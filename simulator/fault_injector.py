import json
import time
import random
import argparse
from datetime import datetime
from confluent_kafka import Producer
from patient_generator import generate_patient_event
import requests
import threading
from database import get_connection

KAFKA_BOOTSTRAP = "localhost:9092"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")

def _send(topic, event):
    producer.produce(
        topic,
        key=event.get("patient_id", "unknown"),
        value=json.dumps(event),
        callback=delivery_report
    )
    producer.poll(0)

# ── Fault 1 ───────────────────────────────────────────────────────
def inject_schema_fault(n_events=20):
    print(f"[FAULT] SCHEMA — sending {n_events} events missing 'spo2' + extra unknown field...")
    for _ in range(n_events):
        event = generate_patient_event()
        event.pop("spo2")                                        # remove required field
        event["diagnosis_code"] = "ICD-" + str(random.randint(1000, 9999))  # add unknown field
        _send("ehr-stream", event)
        time.sleep(0.3)
    producer.flush()
    print("[FAULT] Schema fault done.")

# ── Fault 2 ───────────────────────────────────────────────────────
def inject_data_quality_fault(n_events=30):
    print(f"[FAULT] DATA QUALITY — sending {n_events} physiologically impossible events...")
    for _ in range(n_events):
        event = generate_patient_event()
        event["heart_rate"]  = round(random.uniform(220, 300), 1)
        event["spo2"]        = round(random.uniform(30, 60), 1)
        event["bp_systolic"] = round(random.uniform(200, 280), 1)
        _send("ehr-stream", event)
        time.sleep(0.2)
    producer.flush()
    print("[FAULT] Data quality fault done.")


# ── Fault 3 (UPDATED) ──────────────────────────────────────────────
def inject_performance_fault(duration_seconds=30):
    print(f"[FAULT] PERFORMANCE — Executing heavy Cartesian join to choke Databricks...")
    
    def heavy_query():
        try:
            con = get_connection()
            cursor = con.cursor()
            # A massive cross join to max out warehouse compute
            cursor.execute("""
                SELECT COUNT(*)
                FROM healthcare_db.ehr_stream a
                CROSS JOIN healthcare_db.ehr_stream b
                LIMIT 50000000
            """)
            cursor.close()
            con.close()
        except Exception as e:
            print(f"[DB ERROR] {e}")

    # 1. Fire off the heavy query in the background so it doesn't block the stream
    t = threading.Thread(target=heavy_query)
    t.start()

    # 2. Continue normal event streaming
    print(f"[FAULT] Heavy query running. Continuing normal stream for {duration_seconds}s...")
    start = time.time()
    count = 0
    while time.time() - start < duration_seconds:
        _send("ehr-stream", generate_patient_event())
        time.sleep(1)
        count += 1
        
    print(f"[FAULT] Performance fault stream done.")

# ── Fault 4 ───────────────────────────────────────────────────────
def inject_security_fault(n_events=50):
    print(f"[FAULT] SECURITY — sending {n_events} rapid events from same patient ID...")
    attacker_id = "PT-ATTACKER-0000"
    for _ in range(n_events):
        event = generate_patient_event()
        event["patient_id"] = attacker_id
        _send("ehr-stream", event)
        time.sleep(0.05)   # very fast — 50 events in ~2.5 seconds
    producer.flush()
    print("[FAULT] Security fault done.")


# ── Fault 5 (UPDATED) ──────────────────────────────────────────────
def inject_stall_fault(pause_seconds=45):
    print(f"[FAULT] STALL — Pausing Databricks Sink Connector for {pause_seconds}s...")
    
    # 1. Pause the connector via Kafka Connect REST API
    try:
        requests.put("http://localhost:8083/connectors/databricks-sink/pause")
        print("[FAULT] Connector paused! Building up consumer lag...")
    except Exception as e:
        print(f"[WARNING] Could not reach Kafka Connect REST API: {e}")
        print("[WARNING] Make sure your Databricks Sink connector is named 'databricks-sink' and running on port 8083.")

    # 2. Continue producing events at a normal rate to build lag
    start = time.time()
    count = 0
    while time.time() - start < pause_seconds:
        _send("ehr-stream", generate_patient_event())
        count += 1
        time.sleep(1) 

    print(f"[FAULT] Sent {count} events while stalled. Resuming connector...")
    
    # 3. Resume the connector
    try:
        requests.put("http://localhost:8083/connectors/databricks-sink/resume")
        print("[FAULT] Connector resumed. Watch lag decrease in Kafka UI.")
    except Exception as e:
        print(f"[WARNING] Could not reach Kafka Connect REST API to resume: {e}")

# ── CLI ────────────────────────────────────────────────────────────
FAULTS = {
    "schema":       inject_schema_fault,
    "data_quality": inject_data_quality_fault,
    "performance":  inject_performance_fault,
    "security":     inject_security_fault,
    "stall":        inject_stall_fault,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LARF Fault Injector")
    parser.add_argument("--fault", choices=list(FAULTS.keys()), required=True)
    args = parser.parse_args()

    print(f"\n{'='*50}")
    print(f"  LARF Fault Injector  |  {args.fault.upper()}")
    print(f"  {datetime.utcnow().isoformat()}")
    print(f"{'='*50}\n")

    FAULTS[args.fault]()