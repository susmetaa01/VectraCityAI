# Database Operations Agent

This agent provides natural language to SQL capabilities for database INSERT, UPDATE, and DELETE operations, similar to
the existing `call_db_agent` but specifically designed for data modification operations.

## Features

- **Natural Language to DML**: Convert natural language requests into INSERT, UPDATE, and DELETE SQL statements
- **Safety First**: Built-in safety checks and validation to prevent accidental data loss
- **Preview Mode**: Preview affected records before executing destructive operations
- **Confirmation Required**: Requires explicit confirmation for UPDATE and DELETE operations
- **BigQuery Integration**: Optimized for BigQuery with proper GoogleSQL dialect support

## Architecture

The agent consists of several key components:

### Tools

1. **`generate_insert_sql`**: Converts natural language to INSERT statements
2. **`generate_update_sql`**: Converts natural language to UPDATE statements
3. **`generate_delete_sql`**: Converts natural language to DELETE statements
4. **`validate_dml_operation`**: Validates SQL syntax and checks for dangerous patterns
5. **`preview_affected_records`**: Shows which records would be affected by UPDATE/DELETE
6. **`execute_dml_operation`**: Executes the DML operation with safety checks

### Safety Features

- **Automatic WHERE clause validation**: Warns about missing or overly broad WHERE clauses
- **Dry run validation**: Uses BigQuery dry run to validate syntax before execution
- **Preview functionality**: Shows affected records before destructive operations
- **Confirmation prompts**: Requires user confirmation for UPDATE/DELETE operations
- **Error handling**: Comprehensive error handling and logging

## Usage

### Basic Usage

```python
from google.adk.tools import ToolContext
from google.adk.tools.agent_tool import AgentTool
from agents.data_science.sub_agents.db_operations.agent import db_operations_agent

# Create tool context with database settings
tool_context = ToolContext()
tool_context.state = {
    "all_db_settings": {"use_database": "BigQuery"},
    "database_settings": {
        "bq_ddl_schema": "your_schema_here"
    }
}

# Create agent tool
agent_tool = AgentTool(agent=db_operations_agent)

# Execute request
result = await agent_tool.run_async(
    args={"request": "Insert a new user with name 'John Doe' and email 'john@example.com'"},
    tool_context=tool_context
)
```

### Integration with Main Data Science Agent

The agent is automatically integrated with the main data science agent through the `call_db_operations_agent` tool:

```python
# This tool is available in the main data science agent
await call_db_operations_agent(
    "Update the email address for user ID 123 to 'newemail@example.com'",
    tool_context
)
```

## Example Requests

### INSERT Operations

- "Insert a new user with name 'Alice Smith', email 'alice@example.com', and set active status to true"
- "Add a new order for user 123 with product 'Widget A', quantity 5, and price 29.99"

### UPDATE Operations

- "Update the email address to 'newemail@example.com' for user with ID 123"
- "Set all orders from yesterday to status 'shipped'"
- "Increase the price by 10% for all products in category 'electronics'"

### DELETE Operations

- "Delete all orders from user 456 that are older than 2023-01-01"
- "Remove inactive users who haven't logged in for over a year"

## Configuration

### Environment Variables

- `DB_OPERATIONS_MODEL`: Model to use for SQL generation (defaults to `BASELINE_NL2SQL_MODEL`)
- `GOOGLE_CLOUD_PROJECT`: GCP project ID
- `GOOGLE_CLOUD_LOCATION`: GCP location (default: us-central1)
- `BQ_COMPUTE_PROJECT_ID`: BigQuery compute project
- `BQ_DATA_PROJECT_ID`: BigQuery data project
- `BQ_DATASET_ID`: BigQuery dataset ID

## Safety Considerations

1. **Always test in development first**: Never run directly against production data without testing
2. **Review generated SQL**: Always review the generated SQL before execution
3. **Use transactions**: Consider wrapping operations in transactions when possible
4. **Backup data**: Ensure you have backups before running destructive operations
5. **Limit scope**: Use specific WHERE clauses to limit the scope of operations

## Testing

Run the test script to verify the agent works correctly:

```bash
python agents/data_science/sub_agents/db_operations/test_agent.py
```

## Troubleshooting

### Common Issues

1. **Missing schema**: Ensure `bq_ddl_schema` is properly set in the tool context
2. **Authentication**: Verify GCP credentials are properly configured
3. **Model access**: Ensure the specified model is available and accessible
4. **Permissions**: Verify BigQuery permissions for the target dataset

### Logging

The agent uses Python's logging module. Enable debug logging to see detailed information:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
```
