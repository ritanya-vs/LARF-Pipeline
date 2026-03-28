import json
import os
import numpy as np
from datetime import datetime, timezone

BASELINE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "simulator", "baselines", "ehr_baseline.json"
)

ZSCORE_THRESHOLD = 3.0

def load_baseline() -> dict:
    with open(BASELINE_FILE, "r") as f:
        return json.load(f)

def compute_zscore(value: float, mean: float, std: float) -> float:
    """How many standard deviations is this value from the mean?"""
    if std == 0:
        return 0.0
    return abs((value - mean) / std)

def check_event(event: dict) -> dict:
    """
    Run Z-Score check on a single incoming event.
    Returns a result dict with any anomalous fields flagged.
    """
    baseline = load_baseline()
    anomalies = {}

    for field, stats in baseline["fields"].items():
        if field not in event:
            continue

        value = event[field]
        mean  = stats["mean"]
        std   = stats["std"]
        z     = compute_zscore(value, mean, std)

        if z > ZSCORE_THRESHOLD:
            anomalies[field] = {
                "value":     value,
                "mean":      mean,
                "std":       std,
                "zscore":    round(z, 2),
                "threshold": ZSCORE_THRESHOLD,
            }

    return {
        "detector":       "zscore",
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "patient_id":     event.get("patient_id"),
        "anomalies_found": len(anomalies) > 0,
        "anomalies":      anomalies,
        "fault_type":     "data_quality" if anomalies else None,
    }

def check_batch(events: list) -> dict:
    """
    Run Z-Score check on a batch of events.
    Returns summary of how many events had anomalies.
    """
    results     = [check_event(e) for e in events]
    flagged     = [r for r in results if r["anomalies_found"]]
    all_anomaly_fields = {}

    for r in flagged:
        for field, info in r["anomalies"].items():
            if field not in all_anomaly_fields:
                all_anomaly_fields[field] = []
            all_anomaly_fields[field].append(info["zscore"])

    return {
        "detector":        "zscore",
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "total_events":    len(events),
        "flagged_events":  len(flagged),
        "flag_rate":       round(len(flagged) / len(events), 3) if events else 0,
        "anomalous_fields": {
            field: {
                "count":      len(scores),
                "max_zscore": round(max(scores), 2),
                "avg_zscore": round(sum(scores)/len(scores), 2),
            }
            for field, scores in all_anomaly_fields.items()
        },
        "fault_detected":  len(flagged) > 0,
    }

if __name__ == "__main__":
    # Quick test with one normal and one anomalous event
    from datetime import timezone
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "simulator"))
    from patient_generator import generate_patient_event

    print("=== Testing Z-Score Detector ===\n")

    print("-- Normal event --")
    normal = generate_patient_event(anomalous=False)
    result = check_event(normal)
    print(f"Anomalies found: {result['anomalies_found']}")
    print(f"Anomalies: {result['anomalies']}\n")

    print("-- Anomalous event (data quality fault) --")
    bad = generate_patient_event(anomalous=False)
    bad["heart_rate"]  = 280.0
    bad["spo2"]        = 35.0
    bad["bp_systolic"] = 260.0
    result = check_event(bad)
    print(f"Anomalies found: {result['anomalies_found']}")
    for field, info in result["anomalies"].items():
        print(f"  {field}: value={info['value']}  z-score={info['zscore']}")