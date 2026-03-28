import os
import json
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_classic.agents import AgentExecutor, create_react_agent
from langchain_community.vectorstores import Chroma  # <--- Make sure this line is here!
from langchain_huggingface import HuggingFaceEmbeddings

from tools.schema_tool import execute_sql_ddl
from tools.infra_tool import execute_bash_command
from tools.schema_tool import execute_sql_ddl
from tools.infra_tool import execute_bash_command
from tools.sql_tool import execute_sql_dml
from tools.security_tool import quarantine_patient

from prompts import get_react_prompt

load_dotenv()

class LARFReActAgent:
    def __init__(self):
        # 1. Initialize the LLM (Using Google's LangChain integration)
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-1.5-flash",
            temperature=0.1,
            max_retries=2
        )

        # 2. Load the Tools
        self.tools = [execute_sql_ddl, execute_bash_command, execute_sql_dml, quarantine_patient]
    
        # 3. Setup ChromaDB Retriever (Runbook Memory)
        self.retriever = self._setup_chromadb()

        # 4. Bind the ReAct Prompt
        self.prompt = get_react_prompt()

        # 5. Create the Agent
        self.agent = create_react_agent(self.llm, self.tools, self.prompt)
        
        # The AgentExecutor manages the Thought -> Action -> Observation loop
        self.agent_executor = AgentExecutor(
            agent=self.agent,
            tools=self.tools,
            verbose=True, # Set to True so you can watch the AI "think" in the terminal
            handle_parsing_errors=True, # Crucial: allows the AI to self-correct if it formats a tool input poorly
            max_iterations=5 # Prevents infinite loops if the database is completely down
        )

    def _setup_chromadb(self):
        print("[INFO] Initializing ChromaDB Runbook Retriever (Local Embeddings)...")
        # Use a local open-source model running directly on your Mac!
        embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
        
        runbooks = [
            "RUNBOOK A: If Kafka lag > 100 and CPU is normal, the downstream DB is choking. Action: Use execute_bash_command to run 'python /opt/ehr_producer/control_producer.py --set-rate 500' to throttle the producer.",
            "RUNBOOK B: If schema entropy triggers missing 'ward' field, the producer updated schemas. Action: Use execute_sql_ddl to run 'ALTER TABLE healthcare_db.ehr_stream DROP COLUMN ward;'.",
            "RUNBOOK C: If memory > 80% and DB latency > 2s, Databricks warehouse needs scaling. Action: Notify admin, do not drop tables.",
            "RUNBOOK D: If zscore detects impossible heart rate for a patient, it is a data fault. Action: Use quarantine_patient tool with the rogue patient_id."
        ]
        
        vectorstore = Chroma.from_texts(texts=runbooks, embedding=embeddings)
        return vectorstore.as_retriever(search_kwargs={"k": 1})

    def resolve_crisis(self, crisis_packet):
        print(f"\n[AGENT] Initiating ReAct Loop for {crisis_packet.get('crisis_id')}...")

        # 1. RAG Step: Retrieve relevant runbooks based on the fault signals
        print("[AGENT] Searching runbooks for similar past incidents...")
        context_docs = self.retriever.invoke(str(crisis_packet['fault_signals']))
        runbook_context = "\n".join([doc.page_content for doc in context_docs])
        print(f"[AGENT] Found Runbook: {runbook_context}\n")

        # 2. Inject the packet and the runbook context into the prompt
        packet_str = json.dumps(crisis_packet, indent=2) + f"\n\nHistorical Context:\n{runbook_context}"

        # 3. Trigger the loop
        try:
            response = self.agent_executor.invoke({"crisis_packet": packet_str})
            return response
        except Exception as e:
            print(f"[AGENT FATAL ERROR] {e}")
            return None

if __name__ == "__main__":
    # A quick dummy packet to test the LangChain setup
    test_packet = {
        "crisis_id": "CRISIS-12345",
        "fault_signals": [{"detector": "schema_entropy", "missing_fields": ["ward"]}],
    }
    
    agent = LARFReActAgent()
    agent.resolve_crisis(test_packet)