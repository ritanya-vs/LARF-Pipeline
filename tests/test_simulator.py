import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'simulator'))

import pytest
from patient_generator import generate_patient_event

NORMAL = {
    "heart_rate":       (60, 100),
    "bp_systolic":      (90, 140),
    "bp_diastolic":     (60, 90),
    "spo2":             (95, 100),
    "temperature_c":    (36.1, 37.8),
    "respiratory_rate": (12, 20),
}

def test_all_required_fields_present():
    event = generate_patient_event()
    for field in NORMAL.keys():
        assert field in event, f"Missing: {field}"

def test_normal_values_in_range():
    for _ in range(50):
        event = generate_patient_event(anomalous=False)
        for field, (lo, hi) in NORMAL.items():
            assert lo <= event[field] <= hi, \
                f"{field}={event[field]} outside [{lo}, {hi}]"

def test_anomalous_produces_outliers():
    outliers = 0
    for _ in range(200):
        event = generate_patient_event(anomalous=True)
        for field, (lo, hi) in NORMAL.items():
            if event[field] < lo or event[field] > hi:
                outliers += 1
    assert outliers > 0, "Anomalous mode should produce at least some out-of-range values"

def test_event_ids_are_unique():
    ids = [generate_patient_event()["event_id"] for _ in range(100)]
    assert len(set(ids)) == 100

def test_ward_always_valid():
    valid = {"ICU", "GENERAL", "CARDIO", "NEURO"}
    for _ in range(50):
        assert generate_patient_event()["ward"] in valid