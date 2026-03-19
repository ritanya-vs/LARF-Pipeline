"""
Mock Snowflake connector for Sprints 1-3.
Simulates INSERT behaviour without a real Snowflake account.
Replace with real snowflake-connector-python in Sprint 4.
"""
import json
import os
from datetime import datetime

MOCK_DB_PATH = "mock_snowflake_data"

class MockSnowflakeConnector:
    def __init__(self):
        os.makedirs(MOCK_DB_PATH, exist_ok=True)
        print("[MOCK SNOWFLAKE] Connected to mock Snowflake instance.")

    def insert(self, table: str, record: dict):
        """Simulates inserting a record into a Snowflake table."""
        filepath = os.path.join(MOCK_DB_PATH, f"{table}.jsonl")
        record["_inserted_at"] = datetime.utcnow().isoformat()
        with open(filepath, "a") as f:
            f.write(json.dumps(record) + "\n")

    def query(self, table: str, limit: int = 10):
        """Simulates reading records from a Snowflake table."""
        filepath = os.path.join(MOCK_DB_PATH, f"{table}.jsonl")
        if not os.path.exists(filepath):
            return []
        with open(filepath, "r") as f:
            lines = f.readlines()
        return [json.loads(l) for l in lines[-limit:]]

    def row_count(self, table: str) -> int:
        filepath = os.path.join(MOCK_DB_PATH, f"{table}.jsonl")
        if not os.path.exists(filepath):
            return 0
        with open(filepath, "r") as f:
            return sum(1 for _ in f)


if __name__ == "__main__":
    db = MockSnowflakeConnector()
    db.insert("ehr_stream", {"patient_id": "PT-1234", "heart_rate": 78.4})
    db.insert("ehr_stream", {"patient_id": "PT-5678", "heart_rate": 92.1})
    print(f"Row count: {db.row_count('ehr_stream')}")
    print(f"Last records: {db.query('ehr_stream')}")