import subprocess
from langchain.tools import tool

@tool
def execute_bash_command(command: str) -> str:
    """
    Use this tool to execute system-level bash commands. 
    Useful for throttling producers, restarting Kafka connectors, or modifying infrastructure states.
    """
    print(f"\n[⚙️ INFRA TOOL] Executing Bash Command...")
    print(f"> {command}")
    
    try:
        # We use shell=True to allow the LLM to run complex piped commands if needed
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            output = result.stdout.strip()
            msg = f"SUCCESS: Command executed perfectly. Output: {output}"
            return msg
        else:
            error_msg = f"FAILED: Command exited with error code {result.returncode}. Error details: {result.stderr.strip()}"
            print(f"[❌ ERROR] {error_msg}")
            return error_msg
    except Exception as e:
        error_msg = f"FAILED: System execution error - {str(e)}"
        print(f"[❌ ERROR] {error_msg}")
        return error_msg