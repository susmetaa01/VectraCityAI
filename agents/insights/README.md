# Insights Agent

The Insights Agent is a specialized AI agent designed to generate predictive insights from location-based event data by
leveraging clustering analysis and pattern recognition. It integrates with the existing data science agent to pull
information from BigQuery and performs sophisticated analysis to identify potential risks and generate early warnings.

## Overview

The Insights Agent performs the following key functions:

1. **Data Retrieval**: Pulls event data from BigQuery using the data science agent
2. **Location Clustering**: Groups events by geographical proximity (area and sublocation)
3. **Pattern Analysis**: Identifies recurring themes and correlations in categories and subcategories
4. **Predictive Insights**: Generates actionable insights based on identified patterns
5. **Risk Assessment**: Evaluates and prioritizes insights based on potential impact

## Key Features

### 🎯 Predictive Analytics

- Identifies potential infrastructure failures before they occur
- Detects emerging patterns that could lead to service disruptions
- Provides early warning systems for various risk categories

### 📍 Location-Based Clustering

- Groups events by area and sublocation proximity
- Identifies geographical hotspots of activity
- Analyzes spatial patterns and correlations

### 🔍 Pattern Recognition

- Analyzes category and subcategory relationships
- Detects temporal anomalies and trends
- Identifies cross-category correlations

### ⚠️ Risk Assessment

- Categorizes risks as Low, Medium, High, or Critical
- Provides confidence scores for predictions
- Prioritizes actions based on potential impact

## Example Insights

### Infrastructure Risk - Electrical Grid

```json
{
  "type": "Infrastructure Risk - Electrical Grid",
  "location": "Downtown District - Central Area",
  "insight": "Potential grid failure detected based on 5 clustered power outage reports",
  "confidence": 0.85,
  "risk_level": "High",
  "recommendations": [
    "Immediate inspection of electrical infrastructure",
    "Prepare backup power systems",
    "Alert utility companies"
  ],
  "predicted_outcome": "Widespread power outage likely within 24-48 hours"
}
```

### Environmental Risk - Infrastructure Impact

```json
{
  "type": "Environmental Risk - Infrastructure Impact",
  "location": "Residential Area - Oak Street",
  "insight": "Potential power outage due to fallen trees - 3 related incidents clustered",
  "confidence": 0.75,
  "risk_level": "Medium-High",
  "recommendations": [
    "Preemptive tree trimming in affected areas",
    "Power line inspection",
    "Emergency response team standby"
  ],
  "predicted_outcome": "Localized power outages and road blockages"
}
```

## Architecture

### Components

1. **`agents.py`**: Main insights agent with configuration and setup
2. **`tools.py`**: Core tools for clustering, pattern analysis, and insight generation
3. **`prompts.py`**: Instruction prompts for guiding agent behavior
4. **`example_usage.py`**: Example script demonstrating agent capabilities

### Tools

- **`call_data_science_agent`**: Retrieves data from BigQuery via the data science agent
- **`cluster_by_location`**: Performs geographical clustering of events
- **`analyze_category_patterns`**: Identifies patterns in categories and subcategories
- **`generate_predictive_insights`**: Creates actionable insights from patterns
- **`assess_risk_levels`**: Evaluates and categorizes risk levels

## Usage

### Basic Usage

```python
from agents.insights.agents import insights_agent

# Example queries
queries = [
    "Analyze recent electricity outage patterns and predict potential grid failures",
    "Identify areas with multiple tree fall incidents that could impact power infrastructure",
    "Find clustering patterns of safety incidents and assess public safety risks"
]

# Run the agent (requires proper setup)
result = await insights_agent.run_async(query, callback_context)
```

### Integration with Data Science Agent

The insights agent leverages the existing data science agent to:

- Query BigQuery for event data with location and category information
- Retrieve temporal data for trend analysis
- Access structured data for pattern recognition
- Perform statistical analysis on clustered data

## Data Requirements

The insights agent expects data with the following structure:

- **Location Information**: Area, sublocation, latitude, longitude
- **Category Data**: Primary category and subcategory classifications
- **Temporal Data**: Timestamps for trend analysis
- **Event Details**: Relevant metadata and descriptions

## Configuration

### Environment Variables

- `INSIGHTS_AGENT_MODEL`: Model to use for the insights agent (defaults to `ROOT_AGENT_MODEL`)
- `BQ_DATA_PROJECT_ID`: BigQuery data project ID
- `BQ_COMPUTE_PROJECT_ID`: BigQuery compute project ID
- `GOOGLE_CLOUD_PROJECT`: GCP project ID
- `GOOGLE_CLOUD_LOCATION`: GCP location

### Dependencies

The insights agent depends on:

- Google ADK (Agent Development Kit)
- Data Science Agent
- BigQuery access
- Proper authentication and permissions

## Example Scenarios

### Scenario 1: Electrical Grid Monitoring

- **Input**: Multiple electricity/power outage reports in same area
- **Analysis**: Clustering by location, pattern recognition
- **Output**: Early warning for potential grid failure
- **Action**: Infrastructure inspection, backup preparation

### Scenario 2: Environmental Impact Assessment

- **Input**: Tree fall incidents near power lines
- **Analysis**: Spatial correlation with infrastructure
- **Output**: Risk assessment for power outages
- **Action**: Preemptive maintenance, emergency preparedness

### Scenario 3: Public Safety Monitoring

- **Input**: Clustering of safety-related incidents
- **Analysis**: Pattern analysis and risk evaluation
- **Output**: Public safety risk assessment
- **Action**: Increased security, community alerts

## Benefits

- **Proactive Response**: Early detection enables preventive action
- **Resource Optimization**: Prioritized insights help allocate resources effectively
- **Risk Mitigation**: Identifies potential issues before they escalate
- **Improved Safety**: Enhanced monitoring and response capabilities
- **Cost Reduction**: Prevents costly emergency repairs and service disruptions

## Future Enhancements

- Real-time streaming data integration
- Machine learning model integration for improved predictions
- Advanced geospatial analysis capabilities
- Integration with external weather and infrastructure data
- Automated alert and notification systems
