import json
from crisis_packet import CrisisPacketBuilder
from react_agent import LARFReActAgent

def run_master_orchestrator(mock_alerts):
    print("===================================================")
    print("🌟 LARF ORCHESTRATOR: END-TO-END PIPELINE STARTED 🌟")
    print("===================================================\n")

    # 1. Build the Crisis Packet
    print("[1/3] Assembling Crisis Packet from Detectors...")
    builder = CrisisPacketBuilder()
    for alert in mock_alerts:
        builder.add_alert(alert)
    
    crisis_packet = builder.build()
    print(f"Packet Built: {crisis_packet['crisis_id']} | Severity: {crisis_packet['severity']}\n")

    # 2. Initialize the ReAct Agent
    print("[2/3] Booting up ReAct SRE Agent...")
    agent = LARFReActAgent()

    # 3. Hand off the crisis
    print("[3/3] Handing Crisis Packet to Agent for Autonomous Remediation...\n")
    result = agent.resolve_crisis(crisis_packet)
    
    print("\n===================================================")
    print("✅ CRISIS RESOLVED. RESUMING NORMAL OPERATIONS.")
    print("===================================================")
    if result:
        print(f"\nFinal SRE Report:\n{result.get('output', 'No output generated')}")

if __name__ == "__main__":
    # Simulating a Compound Security/Data Fault scenario
    # (e.g., A rogue device is flooding the DB with impossible heart rates and choking the CPU)
    mock_incoming_alerts = [
        {
            "detector": "zscore",
            "timestamp": "2026-03-28T21:00:00Z",
            "patient_id": "P-ROGUE-99",
            "anomalies": {"heart_rate": {"value": 280, "zscore": 5.4}}
        },
        {
            "detector": "isolation_forest",
            "timestamp": "2026-03-28T21:00:02Z",
            "is_anomaly": True,
            "metrics_analyzed": {"consumer_lag": 150, "cpu_percent": 90.0}
        }
    ]

    run_master_orchestrator(mock_incoming_alerts)