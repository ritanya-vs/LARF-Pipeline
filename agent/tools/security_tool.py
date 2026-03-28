import sys
import os
from langchain.tools import tool

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from simulator.database import get_connection

@tool
def quarantine_patient(patient_id: str) -> str:
    """
    Use this tool to quarantine a specific patient ID when a security fault 
    (like a brute-force data flood) is detected from that user.
    """
    print(f"\n[🛡️ SECURITY TOOL] Quarantining Patient: {patient_id}...")
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Purge the spam records to stabilize the database
        query = f"DELETE FROM healthcare_db.ehr_stream WHERE patient_id = '{patient_id}'"
        cursor.execute(query)
        conn.commit()
        rows = cursor.rowcount
        
        cursor.close()
        conn.close()
        return f"SUCCESS: Patient {patient_id} quarantined. {rows} spam records purged."
    except Exception as e:
        error_msg = f"FAILED: Security quarantine error - {str(e)}"
        print(f"[❌ ERROR] {error_msg}")
        return error_msg