from langchain_core.prompts import PromptTemplate

# 1. System Topology & Baselines
PIPELINE_CONTEXT = """
SYSTEM TOPOLOGY:
- Source: Python IoT/EHR Producer
- Message Broker: Kafka (Topic: ehr-stream)
- Sink: Kafka Connect (databricks-sink)
- Target: Databricks SQL Warehouse (Table: healthcare_db.ehr_stream)

NORMAL BASELINES:
- Consumer Lag: < 30 messages
- DB Latency: < 2.5 seconds
- Expected Schema: 'patient_id', 'heart_rate', 'bp_systolic', 'bp_diastolic', 'spo2', 'temperature_c', 'ward', 'timestamp'
"""

# 2. The Chain of Thought (CoT) ReAct Prompt Template
REACT_COT_TEMPLATE = """
You are an Autonomous Site Reliability Engineer. You have received a CRISIS PACKET detailing pipeline anomalies.
You have access to the following tools to investigate and fix the issue:

{tools}

Use the following format strictly:

Crisis Packet: the input JSON containing the fault signals
Evidence: list the specific metrics, schema drifts, or errors found in the packet
Hypothesis: explain what is fundamentally broken based on the evidence
Confidence: rate your hypothesis (Low/Medium/High)
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action (e.g., a SQL query, a bash command, or a tool argument)
Observation: the result of the action
... (this Action/Action Input/Observation can repeat N times)
Thought: I now know the issue is resolved.
Final Answer: A summary of what was diagnosed and exactly what was executed to fix it.

Context:
{pipeline_context}

Crisis Packet:
{crisis_packet}

Thought: {agent_scratchpad}
"""

def get_react_prompt():
    return PromptTemplate(
        template=REACT_COT_TEMPLATE,
        input_variables=["tools", "tool_names", "crisis_packet", "agent_scratchpad"],
        partial_variables={"pipeline_context": PIPELINE_CONTEXT}
    )