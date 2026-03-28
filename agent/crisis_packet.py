import json
from datetime import datetime, timezone

class CrisisPacketBuilder:
    def __init__(self):
        self.packet = {
            "crisis_id": f"CRISIS-{int(datetime.now(timezone.utc).timestamp())}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "fault_signals": [],
            "affected_components": set(),
            "severity": "UNKNOWN"
        }

    def add_alert(self, alert_dict):
        """Ingests raw alerts from detectors (isolation_forest, schema_entropy, etc.)"""
        detector_name = alert_dict.get("detector", "unknown")
        
        # Map detectors to components
        if detector_name in ["isolation_forest", "zscore"]:
            self.packet["affected_components"].add("Kafka_Consumer")
            self.packet["affected_components"].add("Databricks_Warehouse")
        elif detector_name in ["schema_entropy", "ks_test"]:
            self.packet["affected_components"].add("Data_Producer")
            self.packet["affected_components"].add("Kafka_Topic_ehr-stream")

        self.packet["fault_signals"].append(alert_dict)
        self.packet["severity"] = "CRITICAL" if len(self.packet["fault_signals"]) > 1 else "HIGH"

    def build(self):
        """Returns the finalized, structured Crisis Packet ready for the LLM Agent."""
        # Convert sets to lists for JSON serialization
        final_packet = dict(self.packet)
        final_packet["affected_components"] = list(final_packet["affected_components"])
        return final_packet

if __name__ == "__main__":
    # Quick Test
    builder = CrisisPacketBuilder()
    builder.add_alert({"detector": "schema_entropy", "missing_fields": ["ward"]})
    builder.add_alert({"detector": "isolation_forest", "consumer_lag": 129})
    print(json.dumps(builder.build(), indent=2))