import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from simulator.patient_generator import generate_patient_event
from detectors.zscore_detector import check_event as zscore_check
from detectors.ks_test import run_ks_test
from detectors.schema_entropy import check_event as schema_check


def test_zscore_detects_anomaly():
    event = generate_patient_event()
    event["heart_rate"] = 300
    result = zscore_check(event)
    assert result["anomalies_found"]


def test_zscore_normal_not_flagged():
    event = generate_patient_event()
    result = zscore_check(event)
    assert not result["anomalies_found"]


def test_ks_detects_drift():
    batch = []
    for _ in range(50):
        e = generate_patient_event()
        e["heart_rate"] = 250
        batch.append(e)

    result = run_ks_test(batch)
    assert result["drift_detected"]


def test_schema_detects_change():
    event = generate_patient_event()
    event.pop("spo2")
    event["new_field"] = 1

    result = schema_check(event)
    assert result["anomaly_detected"]