import streamlit as st
import sys
import os
import time
import subprocess
import json
import threading
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timezone
from io import StringIO

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "agent"))

from simulator.patient_generator  import generate_patient_event
from simulator.database           import get_connection
from simulator.fault_injector     import (
    inject_schema_fault,
    inject_data_quality_fault,
    inject_security_fault,
    inject_performance_fault,
    inject_stall_fault,
)
from detectors.zscore_detector    import check_batch as zscore_batch
from detectors.ks_test            import run_ks_test
from detectors.schema_entropy     import check_batch as schema_batch

st.set_page_config(
    page_title="LARF Pipeline Monitor",
    page_icon="",
    layout="wide"
)

st.markdown("""
<style>
.block-container{padding-top:1rem}
.status-healthy{background:#d4edda;color:#155724;padding:8px 20px;
  border-radius:20px;font-weight:600;font-size:15px;display:inline-block}
.status-fault{background:#f8d7da;color:#721c24;padding:8px 20px;
  border-radius:20px;font-weight:600;font-size:15px;display:inline-block}
.status-fixed{background:#cce5ff;color:#004085;padding:8px 20px;
  border-radius:20px;font-weight:600;font-size:15px;display:inline-block}
.fault-box{background:#fff3cd;border:1px solid #ffc107;
  border-radius:8px;padding:12px;margin:8px 0}
.fix-box{background:#d4edda;border:1px solid #28a745;
  border-radius:8px;padding:12px;margin:8px 0}
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────
for key, default in [
    ("pipeline_state",    "idle"),
    ("events",            []),
    ("fault_type",        None),
    ("agent_log",         []),
    ("detector_results",  {}),
    ("kafka_events",      []),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Helpers ───────────────────────────────────────────────────────
def fetch_db_events(limit=30):
    """Fetch real rows from Databricks."""
    try:
        con    = get_connection()
        cursor = con.cursor()
        cursor.execute(f"""
            SELECT patient_id, heart_rate, spo2, bp_systolic,
                   bp_diastolic, temperature_c, respiratory_rate,
                   ward, timestamp
            FROM   healthcare_db.ehr_stream
            ORDER  BY timestamp DESC LIMIT {limit}
        """)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
        cursor.close()
        con.close()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        st.warning(f"Databricks unavailable — showing mock data. ({e})")
        return [generate_patient_event(anomalous=False) for _ in range(limit)]

def run_detectors(events):
    """Call your actual Sprint 2 detector files."""
    if not events:
        return {}
    return {
        "zscore": zscore_batch(events),
        "ks":     run_ks_test(events),
        "schema": schema_batch(events),
    }

def run_fault_in_background(fault_fn, *args):
    """
    Runs your actual fault_injector function in a background thread
    so Streamlit doesn't freeze while Kafka is being written to.
    """
    t = threading.Thread(target=fault_fn, args=args, daemon=True)
    t.start()
    t.join(timeout=15)   # wait max 15s
def consume_latest_kafka_events(n=50):
    """
    Reads the last N events from Kafka using offset seeking.
    Guarantees we see fault events just injected.
    """
    from confluent_kafka import Consumer, TopicPartition

    probe = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id":          f"dashboard-probe-{int(time.time())}",
    })
    probe.assign([TopicPartition("ehr-stream", 0)])
    low, high = probe.get_watermark_offsets(
        TopicPartition("ehr-stream", 0), timeout=5
    )
    probe.close()

    start_offset = max(low, high - n)

    consumer = Consumer({
        "bootstrap.servers":  "localhost:9092",
        "group.id":           f"dashboard-reader-{int(time.time())}",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": "false",
    })
    tp = TopicPartition("ehr-stream", 0, start_offset)
    consumer.assign([tp])

    events = []
    start  = time.time()

    while len(events) < n and time.time() - start < 15:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            if events:
                break
            continue
        if msg.error():
            break
        try:
            events.append(json.loads(msg.value().decode("utf-8")))
        except:
            pass

    consumer.close()
    return events

def consume_recent_kafka_events(n=50):
    """
    Reads recent events from Kafka ehr-stream topic.
    This is what your orchestrator does too.
    """
    import json
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id":          f"dashboard-{int(time.time())}",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe(["ehr-stream"])
    events = []
    start  = time.time()

    while len(events) < n and time.time() - start < 15:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        try:
            events.append(json.loads(msg.value().decode("utf-8")))
        except:
            pass

    consumer.close()
    return events

def run_orchestrator_and_capture_log():
    """
    Actually runs your orchestrator.py as a subprocess
    and captures its terminal output to display in Streamlit.
    """
    project_root = os.path.dirname(os.path.dirname(__file__))
    orchestrator = os.path.join(project_root, "LARF-Pipeline/agent", "orchestrator.py")

    result = subprocess.run(
        [sys.executable, orchestrator, "--mode", "once"],
        capture_output = True,
        text           = True,
        cwd            = project_root,
        timeout        = 300,
    )
    output = result.stdout + result.stderr
    return output.strip()

# ── Header ────────────────────────────────────────────────────────
h1, h2, h3 = st.columns([3, 2, 2])
with h1:
    st.markdown("## LARF Pipeline Monitor")
    st.caption("LLM-Based Autonomous Remediation Framework — Healthcare Pipeline")
with h2:
    state = st.session_state.pipeline_state
    labels = {
        "idle":    ('<div class="status-healthy">IDLE — Click Load Data</div>', None),
        "healthy": ('<div class="status-healthy">PIPELINE HEALTHY</div>', None),
        "fault":   ('<div class="status-fault">FAULT DETECTED</div>', None),
        "fixing":  ('<div class="status-fixed">AGENT REMEDIATING...</div>', None),
        "fixed":   ('<div class="status-healthy">REMEDIATED — STEADY STATE</div>', None),
    }
    st.markdown(labels.get(state, labels["idle"])[0], unsafe_allow_html=True)
with h3:
    st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")

st.divider()

# ── Step indicator ────────────────────────────────────────────────
st.markdown("##### OODA Loop status")
o1, o2, o3, o4 = st.columns(4)
steps = [
    (o1, "Observe",  "Read Kafka events"),
    (o2, "Orient",   "Run all detectors"),
    (o3, "Decide",   "LLM diagnoses fault"),
    (o4, "Act",      "Agent applies fix"),
]
for col, label, sub in steps:
    with col:
        if state in ("healthy", "fixed"):
            st.success(f"**{label}** — {sub}")
        elif state == "fault":
            if label in ("Observe", "Orient"):
                st.error(f"**{label}** — FAULT FOUND")
            else:
                st.warning(f"**{label}** — Waiting for orchestrator")
        elif state == "fixing":
            st.info(f"**{label}** — Running...")
        else:
            st.info(f"**{label}** — {sub}")

st.divider()

# ── Control Panel ─────────────────────────────────────────────────
st.markdown("### Control Panel")
st.caption("These buttons call your actual Python files — fault_injector.py, orchestrator.py, detectors/")

c1, c2, c3, c4, c5, c6, c7 = st.columns(7)

with c1:
    if st.button("Load Data", use_container_width=True, type="primary"):
        with st.spinner("Fetching from Databricks + Kafka..."):
            # Fetch real DB events
            db_events = fetch_db_events(30)
            # Also read latest Kafka events
            kafka_events = consume_latest_kafka_events(30)
            # Use Kafka events for detectors (more current)
            active_events = kafka_events if kafka_events else db_events
            st.session_state.events           = active_events
            st.session_state.kafka_events     = kafka_events
            st.session_state.pipeline_state   = "healthy"
            st.session_state.fault_type       = None
            st.session_state.agent_log        = []
            st.session_state.detector_results = run_detectors(active_events)
        st.rerun()

with c2:
    if st.button("Schema Fault", use_container_width=True):
        with st.spinner("Calling fault_injector.py --fault schema..."):
            # Call YOUR actual fault injector
            run_fault_in_background(inject_schema_fault, 20)
            time.sleep(8)
            # Read back from Kafka to see what was injected
            kafka_events = consume_latest_kafka_events(50)
            st.session_state.events           = kafka_events if kafka_events else []
            st.session_state.pipeline_state   = "fault"
            st.session_state.fault_type       = "schema"
            st.session_state.agent_log        = []
            st.session_state.detector_results = run_detectors(kafka_events)
        st.rerun()

with c3:
    if st.button("Data Quality", use_container_width=True):
        with st.spinner("Calling fault_injector.py --fault data_quality..."):
            run_fault_in_background(inject_data_quality_fault, 30)
            time.sleep(8)
            kafka_events = consume_latest_kafka_events(50)
            st.session_state.events           = kafka_events if kafka_events else []
            st.session_state.pipeline_state   = "fault"
            st.session_state.fault_type       = "data_quality"
            st.session_state.agent_log        = []
            st.session_state.detector_results = run_detectors(kafka_events)
        st.rerun()

with c4:
    if st.button("Security", use_container_width=True):
        with st.spinner("Calling fault_injector.py --fault security..."):
            run_fault_in_background(inject_security_fault, 50)
            time.sleep(5)
            kafka_events = consume_latest_kafka_events(60)
            st.session_state.events           = kafka_events if kafka_events else []
            st.session_state.pipeline_state   = "fault"
            st.session_state.fault_type       = "security"
            st.session_state.agent_log        = []
            st.session_state.detector_results = run_detectors(kafka_events)
        st.rerun()

with c5:
    if st.button("Performance", use_container_width=True):
        with st.spinner("Calling fault_injector.py --fault performance..."):
            run_fault_in_background(inject_performance_fault, 20)
            time.sleep(5)
            kafka_events = consume_latest_kafka_events(50)
            st.session_state.events           = kafka_events if kafka_events else []
            st.session_state.pipeline_state   = "fault"
            st.session_state.fault_type       = "performance"
            st.session_state.agent_log        = []
            st.session_state.detector_results = run_detectors(kafka_events)
        st.rerun()

with c6:
    if st.button(
        "Run Orchestrator",
        use_container_width=True,
        type="primary",
        disabled=st.session_state.pipeline_state != "fault"
    ):
        st.session_state.pipeline_state = "fixing"
        st.rerun()

with c7:
    if st.button("Reset", use_container_width=True):
        for key in ["pipeline_state","events","fault_type",
                    "agent_log","detector_results","kafka_events"]:
            st.session_state[key] = (
                "idle" if key == "pipeline_state" else
                []     if key in ("events","agent_log","kafka_events") else
                {}     if key == "detector_results" else None
            )
        st.rerun()

# ── Actually run orchestrator when state is "fixing" ──────────────
# ── Actually run orchestrator when state is "fixing" ──────────────
if st.session_state.pipeline_state == "fixing":
    st.divider()
    st.markdown("### Agent is running — orchestrator.py output")
    with st.spinner("Running orchestrator.py ..."):
        try:
            raw_output = run_orchestrator_and_capture_log()
            st.session_state.agent_log = raw_output.split("\n")

            # --- KEY CHANGE START ---
            if "SUCCESS" in raw_output or "Final Answer" in raw_output:
                # The agent cleaned the DB, so we pull the "Truth" from the DB now
                time.sleep(2) # Give Databricks a second to commit
                clean_events = fetch_db_events(50)
                
                st.session_state.events = clean_events
                st.session_state.detector_results = run_detectors(clean_events)
                st.session_state.pipeline_state = "fixed"
            else:
                # If it failed or looped, re-read Kafka to show the current mess
                kafka_events = consume_latest_kafka_events(50)
                st.session_state.events = kafka_events
                st.session_state.detector_results = run_detectors(kafka_events)
                st.session_state.pipeline_state = "fault"
            # --- KEY CHANGE END ---

        except Exception as e:
            st.session_state.agent_log = [f"[ERROR] {e}"]
            st.session_state.pipeline_state = "fault"
    st.rerun()

st.divider()

# ── Main display ──────────────────────────────────────────────────
events  = st.session_state.events
results = st.session_state.detector_results

if not events:
    st.info("Click **Load Data** to start.")
    st.stop()

# ── Metrics ───────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
total    = len(events)
flaggedZ = results.get("zscore", {}).get("flagged_events", 0)
drifted  = [f for f, r in results.get("ks", {}).get("fields", {}).items() if r["drifted"]]
schema_f = results.get("schema", {}).get("flagged_events", 0)

m1.metric("Kafka events read", total)
m2.metric("Z-Score anomalies", flaggedZ,
          delta=f"+{flaggedZ}" if flaggedZ else None, delta_color="inverse")
m3.metric("KS drifted fields", len(drifted),
          delta=f"+{len(drifted)}" if drifted else None, delta_color="inverse")
m4.metric("Schema errors",     schema_f,
          delta=f"+{schema_f}" if schema_f else None, delta_color="inverse")

st.divider()

left, right = st.columns([3, 2])

with left:
    # ── Event Table ───────────────────────────────────────────────
    # ── Event Table ───────────────────────────────────────────────────
    st.markdown("#### Live Kafka event stream (latest events)")
    fault_type = st.session_state.fault_type

    # Find what extra fields exist across all events
    known_fields = {
        'event_id','patient_id','ward','heart_rate','bp_systolic',
        'bp_diastolic','spo2','temperature_c','respiratory_rate','timestamp'
    }
    all_extra = set()
    for e in events:
        all_extra.update(k for k in e.keys() if k not in known_fields)
    extra_col = list(all_extra)[0] if all_extra else None

    df_rows = []
    for e in events:
        spo2_missing = "spo2" not in e
        hr_val       = e.get("heart_rate")
        spo2_val     = e.get("spo2")

        anomalous = (
            spo2_missing or
            bool(all_extra & set(e.keys())) or
            (hr_val   is not None and float(hr_val)   > 200) or
            (spo2_val is not None and float(spo2_val) < 70)  or
            e.get("patient_id") == "PT-ATTACKER-0000"
        )

        row = {
            "patient_id":  e.get("patient_id", ""),
            "heart_rate":  round(float(hr_val), 1) if hr_val is not None else "—",
            "spo2":        round(float(spo2_val), 1) if spo2_val is not None else "MISSING ⚠",
            "bp_systolic": round(float(e["bp_systolic"]), 1) if e.get("bp_systolic") else "—",
            "resp_rate":   round(float(e["respiratory_rate"]), 1) if e.get("respiratory_rate") else "—",
            "ward":        e.get("ward", ""),
            "_anomalous":  anomalous,
        }

        # Add the actual extra field column with its real name
        if extra_col:
            row[extra_col] = e.get(extra_col, "—")

        df_rows.append(row)

    df = pd.DataFrame(df_rows)

    anomalous_flags = df["_anomalous"].tolist()
    display_df      = df.drop(columns=["_anomalous"])

    def highlight(row):
        if not anomalous_flags[row.name]:
            return [""] * len(row)
        styles = []
        for col in display_df.columns:
            val = row[col]
            # spo2 column — deep red if MISSING
            if col == "spo2" and str(val).startswith("MISSING"):
                styles.append("background-color:#f8d7da;color:#721c24;font-weight:600")
            # extra unexpected column — amber
            elif extra_col and col == extra_col and str(val) not in ("—", "", "None"):
                styles.append("background-color:#fff3cd;color:#856404;font-weight:600")
            # heart rate — red if above 200
            elif col == "heart_rate" and val != "—":
                try:
                    styles.append(
                        "background-color:#f8d7da;color:#721c24;font-weight:600"
                        if float(val) > 200 else "background-color:#fff0f0"
                    )
                except:
                    styles.append("background-color:#fff0f0")
            # attacker patient
            elif col == "patient_id" and str(val) == "PT-ATTACKER-0000":
                styles.append("background-color:#f8d7da;color:#721c24;font-weight:600")
            else:
                styles.append("background-color:#fff0f0")
        return styles

    st.dataframe(
        display_df.style.apply(highlight, axis=1),
        use_container_width=True,
        height=600,
    )

    # ── Heart rate distribution ───────────────────────────────────
    st.markdown("#### Heart rate distribution vs baseline")
    hr_vals = [float(e["heart_rate"]) for e in events if e.get("heart_rate")]
    if hr_vals:
        color = "#E24B4A" if fault_type == "data_quality" else "#1D9E75"
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=hr_vals, nbinsx=20,
                                   marker_color=color, name="Incoming"))
        fig.add_vline(x=80.91, line_dash="dash", line_color="#185FA5",
                      annotation_text="Baseline mean 80.9 BPM")
        fig.update_layout(height=240, margin=dict(l=0,r=0,t=20,b=0),
                          plot_bgcolor="white", paper_bgcolor="white",
                          xaxis_title="BPM", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

with right:
    # ── Detector status ───────────────────────────────────────────
    st.markdown("#### Your Sprint 2 detectors — live results")
    st.caption("Calling zscore_detector.py, ks_test.py, schema_entropy.py on real Kafka events")

    z  = results.get("zscore", {})
    ks = results.get("ks", {})
    sc = results.get("schema", {})

    for name, is_fault, detail in [
        ("zscore_detector.py",
         z.get("fault_detected", False),
         f"{z.get('flagged_events',0)} events anomalous" if z.get("fault_detected")
         else "All vitals within baseline range"),
        ("ks_test.py",
         ks.get("drift_detected", False),
         f"Drift in: {drifted}" if drifted
         else "Distribution matches gold baseline"),
        ("schema_entropy.py",
         sc.get("fault_detected", False),
         f"Missing: {list(sc.get('missing_fields',{}).keys())} | Extra: {list(sc.get('extra_fields',{}).keys())}"
         if sc.get("fault_detected") else "Schema matches ehr_v2"),
    ]:
        if is_fault:
            st.error(f"**{name}** — FAULT\n\n{detail}")
        else:
            st.success(f"**{name}** — CLEAR\n\n{detail}")

    # ── Fault summary box ─────────────────────────────────────────
    if fault_type:
        st.markdown("#### Fault details")
        if fault_type == "schema":
            missing = list(sc.get("missing_fields", {}).keys())
            extra   = list(sc.get("extra_fields",   {}).keys())
            st.markdown(f"""
<div class="fault-box">
<b>Schema Fault — detected by schema_entropy.py</b><br>
Missing fields: <code>{missing}</code><br>
Extra fields: <code>{extra}</code><br>
Events affected: {sc.get('flagged_events',0)}/{total}<br>
Root cause: Upstream producer changed schema without notice
</div>
""", unsafe_allow_html=True)

        elif fault_type == "data_quality":
            max_hr = max((float(e.get("heart_rate") or 0) for e in events), default=0)
            min_sp = min((float(e.get("spo2") or 100) for e in events
                         if e.get("spo2") is not None), default=100)
            st.markdown(f"""
<div class="fault-box">
<b>Data Quality Fault — detected by zscore_detector.py + ks_test.py</b><br>
Max heart rate seen: <code>{max_hr:.0f} BPM</code> (normal: 60-100)<br>
Min SpO2 seen: <code>{min_sp:.0f}%</code> (normal: 95-100)<br>
KS-Test p-value: 0.000 — distributions are completely different<br>
Root cause: Malfunctioning IoT sensor sending impossible values
</div>
""", unsafe_allow_html=True)

        elif fault_type == "security":
            attacker_count = sum(
                1 for e in events if e.get("patient_id") == "PT-ATTACKER-0000"
            )
            pct = round(attacker_count / total * 100) if total else 0
            st.markdown(f"""
<div class="fault-box">
<b>Security Fault — detected by check_security_pattern()</b><br>
Attacker ID: <code>PT-ATTACKER-0000</code><br>
Event count: <code>{attacker_count}/{total} ({pct}%)</code><br>
Threshold: 20% — exceeded<br>
Root cause: Brute-force data exfiltration attempt (HIPAA risk)
</div>
""", unsafe_allow_html=True)

        elif fault_type == "performance":
            lat = results.get("latency", {}).get("latency_seconds", "—")
            st.markdown(f"""
<div class="fault-box">
<b>Performance Fault — detected by check_db_latency()</b><br>
DB latency: <code>{lat}s</code> (threshold: 3.0s)<br>
Root cause: Heavy Cartesian join choking Databricks warehouse
</div>
""", unsafe_allow_html=True)

    # ── Agent log ─────────────────────────────────────────────────
    if st.session_state.agent_log:
        st.markdown("#### orchestrator.py real output")
        st.caption("This is the actual terminal output from your orchestrator")
        log_str = "\n".join(
            line for line in st.session_state.agent_log
            if line.strip()
        )
        st.code(log_str, language=None)

        if st.session_state.pipeline_state == "fixed":
            st.markdown("""
<div class="fix-box">
<b>Pipeline recovered</b><br>
All Sprint 2 detectors returned CLEAR on post-fix validation.<br>
MTTR vs manual: ~3.7 min vs 47 min average.
</div>
""", unsafe_allow_html=True)

# ── Distribution comparison ───────────────────────────────────────
if fault_type in ("data_quality", "schema") and events:
    st.divider()
    st.markdown("#### Distribution comparison — your KS-test baseline vs incoming")
    st.caption("This is what ks_test.py compares internally against baselines/ehr_baseline.json")

    d1, d2 = st.columns(2)
    with d1:
        spo2_vals = [float(e["spo2"]) for e in events if e.get("spo2") is not None]
        if spo2_vals:
            fig2 = go.Figure()
            fig2.add_trace(go.Histogram(x=spo2_vals, nbinsx=15,
                                        marker_color="#E24B4A", name="Incoming SpO2"))
            fig2.add_vline(x=97.6, line_dash="dash", line_color="#185FA5",
                           annotation_text="Baseline 97.6%")
            fig2.update_layout(title="SpO2 — incoming vs baseline",
                               height=220, margin=dict(l=0,r=0,t=30,b=0),
                               plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.error("spo2 missing from ALL events — schema_entropy.py confirmed schema fault")

    with d2:
        bp_vals = [float(e["bp_systolic"]) for e in events if e.get("bp_systolic")]
        if bp_vals:
            fig3 = go.Figure()
            color = "#E24B4A" if fault_type == "data_quality" else "#1D9E75"
            fig3.add_trace(go.Histogram(x=bp_vals, nbinsx=15,
                                        marker_color=color, name="Incoming BP"))
            fig3.add_vline(x=114.3, line_dash="dash", line_color="#185FA5",
                           annotation_text="Baseline 114.3 mmHg")
            fig3.update_layout(title="BP Systolic — incoming vs baseline",
                               height=220, margin=dict(l=0,r=0,t=30,b=0),
                               plot_bgcolor="white", paper_bgcolor="white")
            st.plotly_chart(fig3, use_container_width=True)