import os
from datetime import datetime
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return sql.connect(
        server_hostname = os.getenv("DATABRICKS_HOST").replace("https://", ""),
        http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
        access_token    = os.getenv("DATABRICKS_TOKEN")
    )

def insert_ehr_event(event: dict):
    con = get_connection()
    cursor = con.cursor()
    cursor.execute("""
        INSERT INTO healthcare_db.ehr_stream
        (event_id, patient_id, ward, heart_rate, bp_systolic,
         bp_diastolic, spo2, temperature_c, timestamp, inserted_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        event["event_id"],
        event["patient_id"],
        event["ward"],
        event["heart_rate"],
        event["bp_systolic"],
        event["bp_diastolic"],
        event["spo2"],
        event["temperature_c"],
        event["timestamp"],
        datetime.utcnow()
    ])
    cursor.close()
    con.close()

def insert_iot_event(event: dict):
    con = get_connection()
    cursor = con.cursor()
    cursor.execute("""
        INSERT INTO healthcare_db.iot_vitals
        (patient_id, heart_rate, spo2, timestamp, inserted_at)
        VALUES (?, ?, ?, ?, ?)
    """, [
        event["patient_id"],
        event["heart_rate"],
        event["spo2"],
        event["timestamp"],
        datetime.utcnow()
    ])
    cursor.close()
    con.close()

def row_count(table: str) -> int:
    con = get_connection()
    cursor = con.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM healthcare_db.{table}")
    count = cursor.fetchone()[0]
    cursor.close()
    con.close()
    return count

if __name__ == "__main__":
    print("[DB] Testing Databricks connection...")

    # Test insert
    test_event = {
        "event_id":      "test-001",
        "patient_id":    "PT-1234",
        "ward":          "ICU",
        "heart_rate":    78.4,
        "bp_systolic":   120.0,
        "bp_diastolic":  80.0,
        "spo2":          98.0,
        "temperature_c": 37.1,
        "timestamp":     datetime.utcnow().isoformat()
    }

    insert_ehr_event(test_event)
    print(f"[DB] Insert successful.")
    print(f"[DB] Rows in ehr_stream: {row_count('ehr_stream')}")