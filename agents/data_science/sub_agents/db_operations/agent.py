"""Database Operations Agent for INSERT, UPDATE, and DELETE operations."""

import os

from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.genai import types

from . import tools
from .prompts import return_instructions_db_operations


def setup_before_agent_call(callback_context: CallbackContext) -> None:
    """Setup the agent before each call."""

    # Ensure database settings are available
    if "database_settings" not in callback_context.state:
        # Import here to avoid circular imports
        from ..bigquery.tools import get_database_settings
        callback_context.state["database_settings"] = get_database_settings()


db_operations_agent = Agent(
    model=os.getenv("DB_OPERATIONS_AGENT_MODEL", os.getenv("BIGQUERY_AGENT_MODEL")),
    name="db_operations_agent",
    instruction=return_instructions_db_operations(),
    tools=[
        tools.generate_insert_sql,
        tools.generate_update_sql,
        tools.generate_delete_sql,
        tools.validate_dml_operation,
        tools.preview_affected_records,
        tools.execute_dml_operation,
    ],
    before_agent_callback=setup_before_agent_call,
    generate_content_config=types.GenerateContentConfig(temperature=0.01),
)
