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

"""Configuration for the insights agent to handle different environments."""

import logging
import os
import sys

logger = logging.getLogger(__name__)


def setup_insights_environment():
    """Setup the environment for the insights agent."""

    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    agents_dir = os.path.dirname(current_dir)
    project_root = os.path.dirname(agents_dir)

    # Add necessary paths to sys.path
    paths_to_add = [
        project_root,  # Project root
        agents_dir,  # Agents directory
        current_dir,  # Current insights directory
    ]

    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)

    logger.info(f"Added paths to sys.path: {paths_to_add}")


def get_data_science_agent():
    """Get the data science agent with proper error handling."""

    # Ensure environment is set up
    setup_insights_environment()

    # Try multiple import strategies
    strategies = [
        _import_strategy_1,
        _import_strategy_2,
        _import_strategy_3,
        _import_strategy_4,
    ]

    for i, strategy in enumerate(strategies, 1):
        try:
            agent = strategy()
            logger.info(f"Successfully imported data science agent using strategy {i}")
            return agent
        except Exception as e:
            logger.debug(f"Strategy {i} failed: {e}")
            continue

    logger.error("All import strategies failed for data science agent")
    return None


def _import_strategy_1():
    """Strategy 1: Direct relative import."""
    from ..data_science.agent import root_agent
    return root_agent


def _import_strategy_2():
    """Strategy 2: Absolute import."""
    from agents.data_science.agent import root_agent
    return root_agent


def _import_strategy_3():
    """Strategy 3: Dynamic import with importlib."""
    import importlib
    module = importlib.import_module('agents.data_science.agent')
    return module.root_agent


def _import_strategy_4():
    """Strategy 4: Manual path-based import."""
    import importlib.util
    import os

    # Get the path to the data science agent
    current_dir = os.path.dirname(os.path.abspath(__file__))
    agents_dir = os.path.dirname(current_dir)
    ds_agent_path = os.path.join(agents_dir, 'data_science', 'agent.py')

    if not os.path.exists(ds_agent_path):
        raise ImportError(f"Data science agent not found at {ds_agent_path}")

    # Load the module
    spec = importlib.util.spec_from_file_location("data_science.agent", ds_agent_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module.root_agent


# Initialize the environment when this module is imported
setup_insights_environment()
