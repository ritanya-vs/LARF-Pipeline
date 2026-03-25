import json
import time
import random
import argparse
from datetime import datetime
from confluent_kafka import Producer
from patient_generator import generate_patient_event

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

# ── Fault 3 ───────────────────────────────────────────────────────
def inject_performance_fault(duration_seconds=30):
    print(f"[FAULT] PERFORMANCE — flooding Kafka for {duration_seconds}s (no sleep)...")
    start = time.time()
    count = 0
    while time.time() - start < duration_seconds:
        event = generate_patient_event()
        _send("ehr-stream", event)
        count += 1
    producer.flush()
    print(f"[FAULT] Performance fault done. Sent {count} events in {duration_seconds}s.")

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

# ── Fault 5 ───────────────────────────────────────────────────────
def inject_stall_fault(pause_seconds=45):
    print(f"[FAULT] STALL — stopping all events for {pause_seconds}s...")
    print(f"[FAULT] Watch consumer lag rise in Kafka UI at http://localhost:8080")
    time.sleep(pause_seconds)
    print("[FAULT] Stall over — sending 5 recovery events...")
    for _ in range(5):
        _send("ehr-stream", generate_patient_event())
    producer.flush()
    print("[FAULT] Stall fault done.")

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