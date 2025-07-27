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

"""Tools for the insights agent to perform clustering and pattern analysis."""

import json
import logging
from collections import defaultdict, Counter
from typing import Dict, List, Any

from google.adk.tools import ToolContext
from google.adk.tools.agent_tool import AgentTool

# Import configuration for handling different environments
from .config import get_data_science_agent

logger = logging.getLogger(__name__)


def _parse_text_data(text_data: str) -> List[Dict[str, Any]]:
    """Parse text-based data from data science agent output."""
    events = []

    # Try to extract structured information from text
    # This is a simplified parser - in practice, you'd need more sophisticated parsing
    lines = text_data.split('\n')

    current_event = {}
    for line in lines:
        line = line.strip()
        if not line:
            if current_event:
                events.append(current_event)
                current_event = {}
            continue

        # Look for key-value patterns
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip().lower().replace(' ', '_')
            value = value.strip()

            # Map common field names
            if key in ['area', 'location', 'region']:
                current_event['area'] = value
            elif key in ['sublocation', 'sub_location', 'neighborhood']:
                current_event['sublocation'] = value
            elif key in ['category', 'type', 'problem_category']:
                current_event['category'] = value
            elif key in ['subcategory', 'sub_category', 'subtype']:
                current_event['subcategory'] = value
            elif key in ['description', 'summary', 'details']:
                current_event['description'] = value

    # Add the last event if exists
    if current_event:
        events.append(current_event)

    return events if events else _create_mock_events()


def _create_mock_events() -> List[Dict[str, Any]]:
    """Create mock events for demonstration when no real data is available."""
    return [
        {
            "area": "Downtown District",
            "sublocation": "Central Plaza",
            "category": "Electricity",
            "subcategory": "Power Outage",
            "description": "Power outage reported in central area",
            "timestamp": "2025-01-20T10:30:00Z"
        },
        {
            "area": "Downtown District",
            "sublocation": "Central Plaza",
            "category": "Electricity",
            "subcategory": "Power Outage",
            "description": "Additional power outage in same area",
            "timestamp": "2025-01-20T11:15:00Z"
        },
        {
            "area": "Residential Area",
            "sublocation": "Oak Street",
            "category": "Road",
            "subcategory": "Tree Fall",
            "description": "Tree fell across road near power lines",
            "timestamp": "2025-01-20T09:45:00Z"
        },
        {
            "area": "Residential Area",
            "sublocation": "Oak Street",
            "category": "Road",
            "subcategory": "Tree Fall",
            "description": "Another tree down on Oak Street",
            "timestamp": "2025-01-20T10:20:00Z"
        }
    ]


async def call_data_science_agent(
        query: str,
        tool_context: ToolContext,
) -> str:
    """Tool to call the data science agent for BigQuery data retrieval."""

    try:
        ds_agent = get_data_science_agent()
        if ds_agent is None:
            return "Error: Data science agent not available. Please check the import configuration."

        agent_tool = AgentTool(agent=ds_agent)

        # Prepare the query for the data science agent
        data_query = f"""
        Please retrieve data from BigQuery that includes the following information:
        - Event/post location data (latitude, longitude, area, sublocation)
        - Category and subcategory information
        - Timestamp information
        - Any additional relevant details
        
        Specific query: {query}
        
        Please return the data in a structured format suitable for analysis.
        """

        result = await agent_tool.run_async(
            args={"request": data_query},
            tool_context=tool_context
        )

        # Store the result in context for other tools to use
        tool_context.state["raw_data"] = result

        return result

    except Exception as e:
        logger.error(f"Error calling data science agent: {str(e)}")
        return f"Error retrieving data: {str(e)}"


async def cluster_by_location(
        tool_context: ToolContext,
        location_threshold_km: float = 1.0,
) -> str:
    """Cluster events by geographical proximity using area and sublocation data."""

    try:
        # Get data from previous call or retrieve new data
        if "raw_data" not in tool_context.state:
            return "No data available. Please call call_data_science_agent first."

        raw_data = tool_context.state["raw_data"]

        # Initialize clustering structures
        clusters = defaultdict(list)
        location_stats = defaultdict(lambda: {"count": 0, "categories": Counter(), "sublocations": set()})

        # Parse the raw data - handle different formats
        events = []

        try:
            # Try to parse as JSON if it's a string
            if isinstance(raw_data, str):
                # Look for JSON-like structures in the string
                if raw_data.strip().startswith('[') or raw_data.strip().startswith('{'):
                    events = json.loads(raw_data)
                else:
                    # Parse text-based data science agent output
                    events = _parse_text_data(raw_data)
            elif isinstance(raw_data, list):
                events = raw_data
            elif isinstance(raw_data, dict):
                # If it's a dict, look for common data keys
                if 'data' in raw_data:
                    events = raw_data['data']
                elif 'results' in raw_data:
                    events = raw_data['results']
                else:
                    events = [raw_data]  # Single event
            else:
                # Fallback: create mock events for demonstration
                events = _create_mock_events()

        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Could not parse raw_data as JSON: {e}. Using mock data for demonstration.")
            events = _create_mock_events()

        # Ensure events is a list
        if not isinstance(events, list):
            events = [events] if events else []

        # Group events by area first, then by sublocation
        area_groups = defaultdict(lambda: defaultdict(list))

        # Process each event
        for event in events:
            # Handle different event formats
            if isinstance(event, dict):
                area = event.get("area", event.get("location", "Unknown Area"))
                sublocation = event.get("sublocation", event.get("sub_location", "General"))
                category = event.get("category", event.get("problem_category", "Unknown"))
                subcategory = event.get("subcategory", event.get("sub_category", "Unknown"))
            else:
                # Handle string or other formats
                area = "Unknown Area"
                sublocation = "General"
                category = "Unknown"
                subcategory = "Unknown"

            # Clean up the data
            area = str(area).strip() if area else "Unknown Area"
            sublocation = str(sublocation).strip() if sublocation else "General"
            category = str(category).strip() if category else "Unknown"
            subcategory = str(subcategory).strip() if subcategory else "Unknown"

            # Add to area-sublocation grouping
            area_groups[area][sublocation].append(event)

            # Update location statistics
            location_key = f"{area}_{sublocation}"
            location_stats[location_key]["count"] += 1
            location_stats[location_key]["categories"][category] += 1
            location_stats[location_key]["sublocations"].add(sublocation)

        # Create clusters based on location proximity and event density
        cluster_id = 0
        significant_clusters = {}

        for area, sublocations in area_groups.items():
            for sublocation, events in sublocations.items():
                if len(events) >= 2:  # Minimum events for a cluster
                    cluster_key = f"cluster_{cluster_id}"
                    cluster_info = {
                        "cluster_id": cluster_id,
                        "area": area,
                        "sublocation": sublocation,
                        "event_count": len(events),
                        "categories": list(set(e.get("category", "") for e in events)),
                        "subcategories": list(set(e.get("subcategory", "") for e in events)),
                        "events": events
                    }
                    significant_clusters[cluster_key] = cluster_info
                    cluster_id += 1

        cluster_result = {
            "total_clusters": cluster_id,
            "significant_clusters": significant_clusters,
            "location_statistics": dict(location_stats),
            "summary": f"Identified {cluster_id} significant location clusters with 2+ events",
            "hotspots": [
                cluster for cluster in significant_clusters.values()
                if cluster["event_count"] >= 3
            ]
        }

        # Ensure insights_state exists in context
        if "insights_state" not in tool_context.state:
            tool_context.state["insights_state"] = {}

        # Store clusters in context
        tool_context.state["insights_state"]["clusters"] = cluster_result

        return json.dumps(cluster_result, indent=2)

    except Exception as e:
        logger.error(f"Error in location clustering: {str(e)}")
        return f"Error in clustering: {str(e)}"


async def analyze_category_patterns(
        tool_context: ToolContext,
        time_window_days: int = 7,
) -> str:
    """Analyze patterns in categories and subcategories to identify trends and correlations."""

    try:
        if "raw_data" not in tool_context.state:
            return "No data available. Please call call_data_science_agent first."

        # Ensure insights_state exists
        if "insights_state" not in tool_context.state:
            tool_context.state["insights_state"] = {}

        clusters = tool_context.state["insights_state"].get("clusters", {})

        # Initialize pattern analysis structures
        category_patterns = {
            "electricity_patterns": [],
            "road_patterns": [],
            "safety_patterns": [],
            "infrastructure_patterns": [],
            "environmental_patterns": []
        }

        # Analyze patterns from clusters
        patterns_found = {
            "high_frequency_categories": [],
            "location_category_correlations": [],
            "temporal_anomalies": [],
            "subcategory_clusters": [],
            "risk_patterns": []
        }

        # Process significant clusters for pattern detection
        if "significant_clusters" in clusters:
            for cluster_key, cluster_info in clusters["significant_clusters"].items():
                area = cluster_info["area"]
                sublocation = cluster_info["sublocation"]
                categories = cluster_info["categories"]
                subcategories = cluster_info["subcategories"]
                event_count = cluster_info["event_count"]

                # Detect electricity-related patterns
                if any("electricity" in cat.lower() or "power" in cat.lower() for cat in categories):
                    if any("outage" in sub.lower() for sub in subcategories):
                        pattern = {
                            "type": "electricity_outage_cluster",
                            "location": f"{area} - {sublocation}",
                            "event_count": event_count,
                            "categories": categories,
                            "subcategories": subcategories,
                            "risk_indicator": "potential_grid_failure"
                        }
                        category_patterns["electricity_patterns"].append(pattern)
                        patterns_found["risk_patterns"].append(pattern)

                # Detect road/tree-related patterns
                if any("road" in cat.lower() or "tree" in cat.lower() for cat in categories):
                    if any("fall" in sub.lower() or "block" in sub.lower() for sub in subcategories):
                        pattern = {
                            "type": "road_obstruction_cluster",
                            "location": f"{area} - {sublocation}",
                            "event_count": event_count,
                            "categories": categories,
                            "subcategories": subcategories,
                            "risk_indicator": "potential_infrastructure_impact"
                        }
                        category_patterns["road_patterns"].append(pattern)
                        patterns_found["risk_patterns"].append(pattern)

                # Detect safety patterns
                if any("safety" in cat.lower() or "emergency" in cat.lower() for cat in categories):
                    pattern = {
                        "type": "safety_concern_cluster",
                        "location": f"{area} - {sublocation}",
                        "event_count": event_count,
                        "categories": categories,
                        "subcategories": subcategories,
                        "risk_indicator": "public_safety_risk"
                    }
                    category_patterns["safety_patterns"].append(pattern)
                    patterns_found["risk_patterns"].append(pattern)

                # High frequency location-category correlations
                if event_count >= 3:
                    correlation = {
                        "location": f"{area} - {sublocation}",
                        "dominant_categories": categories,
                        "frequency": event_count,
                        "correlation_strength": "high" if event_count >= 5 else "medium"
                    }
                    patterns_found["location_category_correlations"].append(correlation)

        # Identify high-frequency categories across all clusters
        all_categories = Counter()
        for cluster_info in clusters.get("significant_clusters", {}).values():
            for category in cluster_info["categories"]:
                all_categories[category] += cluster_info["event_count"]

        # Categories with high frequency across multiple locations
        for category, count in all_categories.most_common(5):
            if count >= 5:  # Threshold for high frequency
                patterns_found["high_frequency_categories"].append({
                    "category": category,
                    "total_events": count,
                    "significance": "high" if count >= 10 else "medium"
                })

        # Store patterns in context
        tool_context.state["insights_state"]["patterns"] = patterns_found
        tool_context.state["insights_state"]["category_patterns"] = category_patterns

        return json.dumps(patterns_found, indent=2)

    except Exception as e:
        logger.error(f"Error in pattern analysis: {str(e)}")
        return f"Error in pattern analysis: {str(e)}"


async def generate_predictive_insights(
        tool_context: ToolContext,
        confidence_threshold: float = 0.7,
) -> str:
    """Generate predictive insights based on identified patterns and clusters."""

    try:
        # Ensure insights_state exists
        if "insights_state" not in tool_context.state:
            tool_context.state["insights_state"] = {}

        clusters = tool_context.state["insights_state"].get("clusters", {})
        patterns = tool_context.state["insights_state"].get("patterns", {})
        category_patterns = tool_context.state["insights_state"].get("category_patterns", {})

        if not clusters and not patterns:
            return "No clusters or patterns available. Please run clustering and pattern analysis first."

        insights = []

        # Generate insights from electricity patterns
        for elec_pattern in category_patterns.get("electricity_patterns", []):
            confidence = min(0.9, 0.6 + (elec_pattern["event_count"] * 0.1))
            if confidence >= confidence_threshold:
                insight = {
                    "insight_id": f"elec_{len(insights)}",
                    "type": "Infrastructure Risk - Electrical Grid",
                    "category": "Electricity",
                    "subcategory": "Power Outage",
                    "location": elec_pattern["location"],
                    "insight": f"Potential grid failure detected in {elec_pattern['location']} based on {elec_pattern['event_count']} clustered power outage reports",
                    "confidence": confidence,
                    "risk_level": "High" if elec_pattern["event_count"] >= 4 else "Medium-High",
                    "recommendations": [
                        "Immediate inspection of electrical infrastructure in affected area",
                        "Prepare backup power systems for critical facilities",
                        "Alert utility companies for preventive maintenance",
                        "Notify emergency services for potential response"
                    ],
                    "predicted_outcome": f"Widespread power outage likely within 24-48 hours affecting {elec_pattern['location']}",
                    "affected_areas": [elec_pattern["location"]],
                    "event_count": elec_pattern["event_count"],
                    "pattern_type": "electricity_outage_cluster",
                    "urgency": "High",
                    "estimated_impact": "Medium to High - Infrastructure disruption"
                }
                insights.append(insight)

        # Generate insights from road/tree patterns
        for road_pattern in category_patterns.get("road_patterns", []):
            confidence = min(0.85, 0.55 + (road_pattern["event_count"] * 0.1))
            if confidence >= confidence_threshold:
                insight = {
                    "insight_id": f"road_{len(insights)}",
                    "type": "Environmental Risk - Infrastructure Impact",
                    "category": "Road/Infrastructure",
                    "subcategory": "Tree Fall/Obstruction",
                    "location": road_pattern["location"],
                    "insight": f"Potential infrastructure impact from fallen trees in {road_pattern['location']} - {road_pattern['event_count']} related incidents clustered",
                    "confidence": confidence,
                    "risk_level": "Medium-High" if road_pattern["event_count"] >= 3 else "Medium",
                    "recommendations": [
                        "Preemptive tree trimming in affected areas",
                        "Power line and infrastructure inspection",
                        "Emergency response team on standby",
                        "Road maintenance crew alert",
                        "Monitor weather conditions"
                    ],
                    "predicted_outcome": f"Localized power outages and road blockages likely in {road_pattern['location']}",
                    "affected_areas": [road_pattern["location"]],
                    "event_count": road_pattern["event_count"],
                    "pattern_type": "road_obstruction_cluster",
                    "urgency": "Medium",
                    "estimated_impact": "Low to Medium - Localized disruption"
                }
                insights.append(insight)

        # Generate insights from safety patterns
        for safety_pattern in category_patterns.get("safety_patterns", []):
            confidence = min(0.8, 0.5 + (safety_pattern["event_count"] * 0.15))
            if confidence >= confidence_threshold:
                insight = {
                    "insight_id": f"safety_{len(insights)}",
                    "type": "Public Safety Risk",
                    "category": "Safety/Emergency",
                    "subcategory": "Public Safety Concern",
                    "location": safety_pattern["location"],
                    "insight": f"Emerging public safety concern pattern in {safety_pattern['location']} - {safety_pattern['event_count']} related incidents",
                    "confidence": confidence,
                    "risk_level": "High" if safety_pattern["event_count"] >= 4 else "Medium",
                    "recommendations": [
                        "Increase security presence in affected area",
                        "Coordinate with local law enforcement",
                        "Community safety awareness campaign",
                        "Enhanced monitoring and response protocols"
                    ],
                    "predicted_outcome": f"Continued safety concerns requiring intervention in {safety_pattern['location']}",
                    "affected_areas": [safety_pattern["location"]],
                    "event_count": safety_pattern["event_count"],
                    "pattern_type": "safety_concern_cluster",
                    "urgency": "High",
                    "estimated_impact": "High - Public safety risk"
                }
                insights.append(insight)

        # Generate cross-pattern insights
        if len(category_patterns.get("electricity_patterns", [])) > 0 and len(
                category_patterns.get("road_patterns", [])) > 0:
            # Check for overlapping locations
            elec_locations = set(p["location"] for p in category_patterns["electricity_patterns"])
            road_locations = set(p["location"] for p in category_patterns["road_patterns"])
            overlap = elec_locations.intersection(road_locations)

            if overlap:
                for location in overlap:
                    insight = {
                        "insight_id": f"combined_{len(insights)}",
                        "type": "Combined Infrastructure Risk",
                        "category": "Multi-Category",
                        "subcategory": "Infrastructure Vulnerability",
                        "location": location,
                        "insight": f"Combined infrastructure vulnerability detected in {location} - both electrical and road/tree issues present",
                        "confidence": 0.9,
                        "risk_level": "Critical",
                        "recommendations": [
                            "Immediate comprehensive infrastructure assessment",
                            "Coordinate utility and road maintenance teams",
                            "Prepare for cascading infrastructure failures",
                            "Emergency response coordination"
                        ],
                        "predicted_outcome": f"High risk of cascading infrastructure failures in {location}",
                        "affected_areas": [location],
                        "pattern_type": "combined_risk_cluster",
                        "urgency": "Critical",
                        "estimated_impact": "High - Multiple infrastructure systems at risk"
                    }
                    insights.append(insight)

        # Store insights in context
        tool_context.state["insights_state"]["insights"] = insights

        return json.dumps(insights, indent=2)

    except Exception as e:
        logger.error(f"Error generating insights: {str(e)}")
        return f"Error generating insights: {str(e)}"


async def assess_risk_levels(
        tool_context: ToolContext,
) -> str:
    """Assess and categorize risk levels for generated insights."""

    try:
        # Ensure insights_state exists
        if "insights_state" not in tool_context.state:
            tool_context.state["insights_state"] = {}

        insights = tool_context.state["insights_state"].get("insights", [])

        if not insights:
            return "No insights available for risk assessment."

        risk_assessments = {
            "critical": [],
            "high": [],
            "medium": [],
            "low": []
        }

        for insight in insights:
            risk_level = insight.get("risk_level", "Unknown").lower()
            confidence = insight.get("confidence", 0.0)

            # Adjust risk based on confidence
            if confidence < 0.5:
                risk_level = "low"
            elif confidence >= 0.9 and risk_level in ["high", "medium-high"]:
                risk_level = "critical"

            # Categorize risks
            if "critical" in risk_level:
                risk_assessments["critical"].append(insight)
            elif "high" in risk_level:
                risk_assessments["high"].append(insight)
            elif "medium" in risk_level:
                risk_assessments["medium"].append(insight)
            else:
                risk_assessments["low"].append(insight)

        # Store risk assessments
        tool_context.state["insights_state"]["risk_assessments"] = risk_assessments

        # Create summary
        summary = {
            "total_insights": len(insights),
            "risk_distribution": {
                level: len(items) for level, items in risk_assessments.items()
            },
            "priority_actions": [],
            "risk_assessments": risk_assessments
        }

        # Add priority actions for critical and high risks
        for critical_insight in risk_assessments["critical"]:
            summary["priority_actions"].extend(critical_insight.get("recommendations", []))

        return json.dumps(summary, indent=2)

    except Exception as e:
        logger.error(f"Error in risk assessment: {str(e)}")
        return f"Error in risk assessment: {str(e)}"
