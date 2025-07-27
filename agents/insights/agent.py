# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Insights Agent for generating predictive insights from clustered data.

This agent pulls information from BigQuery using the data science agent and performs:
1. Location-based clustering of events/posts
2. Category and subcategory pattern analysis
3. Predictive insight generation based on patterns
4. Risk assessment and early warning detection
"""

import os
from datetime import date

from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.adk.tools import load_artifacts
from google.genai import types

from .prompts import return_instructions_insights
from .tools import (
    cluster_by_location,
    analyze_category_patterns,
    generate_predictive_insights,
    call_data_science_agent,
    assess_risk_levels,
)

date_today = date.today()


def setup_before_agent_call(callback_context: CallbackContext):
    """Setup the insights agent with necessary context."""

    # Initialize insights state if not present
    if "insights_state" not in callback_context.state:
        callback_context.state["insights_state"] = {
            "clusters": {},
            "patterns": {},
            "insights": [],
            "risk_assessments": {}
        }

    # Set up data science agent context for BigQuery access
    if "database_settings" not in callback_context.state:
        db_settings = dict()
        db_settings["use_database"] = "BigQuery"
        callback_context.state["all_db_settings"] = db_settings


root_agent = Agent(
    model=os.getenv("INSIGHTS_AGENT_MODEL", os.getenv("ROOT_AGENT_MODEL")),
    name="insights_multiagent",
    instruction=return_instructions_insights(),
    global_instruction=(
        f"""
        You are an Insights Generation Agent specialized in analyzing patterns
        and generating predictive insights from location-based event data.
        Today's date: {date_today}

        Your primary capabilities include:
        - Clustering events by location and sublocation
        - Analyzing category and subcategory patterns
        - Generating predictive insights and early warnings
        - Risk assessment and trend analysis
        """
    ),
    tools=[
        cluster_by_location,
        analyze_category_patterns,
        generate_predictive_insights,
        call_data_science_agent,
        assess_risk_levels,
        load_artifacts,
    ],
    before_agent_callback=setup_before_agent_call,
    generate_content_config=types.GenerateContentConfig(temperature=0.3),
)
