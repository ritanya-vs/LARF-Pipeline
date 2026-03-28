import sys
import os
from langchain.tools import tool

# Ensure we can import from the simulator folder
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from simulator.database import get_connection

@tool
def execute_sql_dml(query: str) -> str:
    """
    Use this tool to execute SQL DML commands (UPDATE, DELETE, INSERT).
    Useful for fixing data quality issues, dropping corrupted rows, or updating specific patient records.
    """
    print(f"\n[🛠️ SQL TOOL] Executing DML Query...")
    print(f"> {query}")
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        rows = cursor.rowcount
        cursor.close()
        conn.close()
        return f"SUCCESS: Query executed. {rows} rows affected."
    except Exception as e:
        error_msg = f"FAILED: Database error occurred - {str(e)}"
        print(f"[❌ ERROR] {error_msg}")
        return error_msg