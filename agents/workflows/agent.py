import os

from data_science.agent import root_agent as data_science_agent
from data_science.sub_agents.bigquery.tools import run_bigquery_validation
from google.adk.agents import LlmAgent, SequentialAgent

from .prompts import clustering_prompt, insights_prompt

data_clustering_agent = LlmAgent(
    name="DataClusteringAgent",
    model=os.getenv("WORKFLOW_AGENT_MODEL", os.getenv("ROOT_AGENT_MODEL")),
    instruction=clustering_prompt(),
    description="Get data in raw format",
    tools=[run_bigquery_validation],
    disallow_transfer_to_parent=True,
    output_key="clustered_data"  # Stores output in state['generated_code']
)

insights_agent = LlmAgent(
    name="InsightsGenerationAgent",
    model=os.getenv("WORKFLOW_AGENT_MODEL", os.getenv("ROOT_AGENT_MODEL")),
    instruction=insights_prompt(),
    description="Find a list of emerging patterns, potential risks, and provide early warning systems based on clustered data analysis",
    disallow_transfer_to_parent=True,
    output_key="insights"  # Stores output in state['generated_code']
)

# --- 2. Create the SequentialAgent ---
# This agent orchestrates the pipeline by running the sub_agents in order.
analysis_agent = SequentialAgent(
    name="AnalysisAgent",
    sub_agents=[data_clustering_agent, insights_agent],
    description="When user asks to run analysis, run the agents in order, passing output from first to second",
    # The agents will run in the order provided: Writer -> Reviewer -> Refactorer
)

# For ADK tools compatibility, the root agent must be named `root_agent`
root_agent = analysis_agent
