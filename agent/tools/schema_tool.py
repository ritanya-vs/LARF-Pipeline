import sys
import os
from langchain.tools import tool

# Ensure we can import from the simulator folder
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from simulator.database import get_connection

@tool
def execute_sql_ddl(command: str) -> str:
    """
    Use this tool to execute SQL DDL commands (like ALTER TABLE, CREATE, DROP) 
    to fix database schema drift issues in Databricks.
    """
    print(f"\n[🔧 SCHEMA TOOL] Executing SQL...")
    print(f"> {command}")
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(command)
        conn.commit()
        cursor.close()
        conn.close()
        return "SUCCESS: SQL command executed successfully. The schema has been updated."
    except Exception as e:
        error_msg = f"FAILED: Database error occurred - {str(e)}"
        print(f"[❌ ERROR] {error_msg}")
        return error_msg