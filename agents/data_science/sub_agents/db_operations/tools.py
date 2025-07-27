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

"""Database operations tools for INSERT, UPDATE, and DELETE operations."""

import logging
import os
import re
from typing import Dict, Any

from google.adk.tools import ToolContext
from google.cloud import bigquery
from google.genai import Client

# Import existing BigQuery utilities
from ..bigquery.tools import get_bq_client, get_database_settings

# Initialize LLM client
vertex_project = os.getenv("GOOGLE_CLOUD_PROJECT", None)
location = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
llm_client = Client(vertexai=True, project=vertex_project, location=location)


def generate_insert_sql(
        request: str,
        tool_context: ToolContext,
) -> str:
    """Generate INSERT SQL from natural language request.

    Args:
        request (str): Natural language description of the INSERT operation
        tool_context (ToolContext): The tool context containing database settings

    Returns:
        str: Generated INSERT SQL statement
    """

    database_settings = tool_context.state.get("database_settings", get_database_settings())
    schema = database_settings.get("bq_ddl_schema", "")

    prompt_template = """
You are a BigQuery SQL expert. Generate an INSERT statement based on the user's request.

**Database Schema:**
{schema}

**User Request:**
{request}

**Guidelines:**
- Generate only the INSERT SQL statement
- Use proper BigQuery syntax and GoogleSQL dialect
- Include all required columns based on the schema
- Use appropriate data types and formatting
- For missing values, use appropriate defaults or NULL
- Ensure the statement is syntactically correct
- Use proper table references with project.dataset.table format if needed
- Handle date/time formats correctly for BigQuery

**GEOGRAPHY DATA HANDLING:**
- For GEOGRAPHY columns, use ST_GEOGPOINT(longitude, latitude) function
- When user provides "latitude X, longitude Y" or "lat X, lng Y", use ST_GEOGPOINT(X, Y)
- When user provides coordinates as "X, Y", assume X is latitude and Y is longitude, so use ST_GEOGPOINT(X, Y)
- ST_GEOGPOINT function signature: ST_GEOGPOINT(latitude_first, longitude_second)
- Example: For latitude 37.7749, longitude -122.4194, use ST_GEOGPOINT(37.7749, -122.4194)

**OTHER DATA TYPE HANDLING:**
- If the column is of timestamp, and the provided input is in string, convert the string to ISO time format.

**Important:** Output only the SQL statement without any markdown formatting, explanations, or additional text.

INSERT SQL:
"""

    logging.info(f"Generating INSERT SQL for request: {request}")

    prompt = prompt_template.format(schema=schema, request=request)

    try:
        response = llm_client.models.generate_content(
            model=os.getenv("DB_OPERATIONS_MODEL", os.getenv("BASELINE_NL2SQL_MODEL", "gemini-2.5-flash")),
            contents=prompt,
            config={"temperature": 0.1},
        )

        sql = response.text
        if sql:
            # Clean up the response
            sql = sql.replace("```sql", "").replace("```", "").strip()
            # Remove any leading explanatory text
            lines = sql.split('\n')
            sql_lines = []
            found_insert = False
            for line in lines:
                if line.strip().upper().startswith('INSERT') or found_insert:
                    found_insert = True
                    sql_lines.append(line)
            sql = '\n'.join(sql_lines).strip()

        logging.info(f"Generated INSERT SQL: {sql}")
        tool_context.state["generated_insert_sql"] = sql
        return sql

    except Exception as e:
        logging.error(f"Error generating INSERT SQL: {str(e)}")
        return f"-- Error generating INSERT SQL: {str(e)}"


def generate_update_sql(
        request: str,
        tool_context: ToolContext,
) -> str:
    """Generate UPDATE SQL from natural language request.

    Args:
        request (str): Natural language description of the UPDATE operation
        tool_context (ToolContext): The tool context containing database settings

    Returns:
        str: Generated UPDATE SQL statement
    """

    database_settings = tool_context.state.get("database_settings", get_database_settings())
    schema = database_settings.get("bq_ddl_schema", "")

    prompt_template = """
You are a BigQuery SQL expert. Generate an UPDATE statement based on the user's request.

**Database Schema:**
{schema}

**User Request:**
{request}

**Guidelines:**
- Generate only the UPDATE SQL statement
- Use proper BigQuery syntax and GoogleSQL dialect
- Include appropriate WHERE clause to limit scope - NEVER create UPDATE without WHERE clause
- Use proper data types and formatting
- Ensure the statement is syntactically correct
- Be conservative with WHERE conditions to prevent accidental mass updates
- Use proper table references with project.dataset.table format if needed
- Handle date/time formats correctly for BigQuery

**GEOGRAPHY DATA HANDLING:**
- For GEOGRAPHY columns, use ST_GEOGPOINT(longitude, latitude) function
- When user provides "latitude X, longitude Y" or "lat X, lng Y", use ST_GEOGPOINT(X, Y)
- When user provides coordinates as "X, Y", assume X is latitude and Y is longitude, so use ST_GEOGPOINT(X, Y)
- ST_GEOGPOINT function signature: ST_GEOGPOINT(latitude_first, longitude_second)
- Example: For latitude 37.7749, longitude -122.4194, use ST_GEOGPOINT(37.7749, -122.4194)

**OTHER DATA TYPE HANDLING:**
- If the column is of timestamp, and the provided input is in string, convert the string to ISO time format.

**SAFETY REQUIREMENT:** Always include a specific WHERE clause. Never generate UPDATE statements that would affect all rows.

**Important:** Output only the SQL statement without any markdown formatting, explanations, or additional text.

UPDATE SQL:
"""

    logging.info(f"Generating UPDATE SQL for request: {request}")

    prompt = prompt_template.format(schema=schema, request=request)

    try:
        response = llm_client.models.generate_content(
            model=os.getenv("DB_OPERATIONS_MODEL", os.getenv("BASELINE_NL2SQL_MODEL", "gemini-2.5-flash")),
            contents=prompt,
            config={"temperature": 0.1},
        )

        sql = response.text
        if sql:
            # Clean up the response
            sql = sql.replace("```sql", "").replace("```", "").strip()
            # Remove any leading explanatory text
            lines = sql.split('\n')
            sql_lines = []
            found_update = False
            for line in lines:
                if line.strip().upper().startswith('UPDATE') or found_update:
                    found_update = True
                    sql_lines.append(line)
            sql = '\n'.join(sql_lines).strip()

        logging.info(f"Generated UPDATE SQL: {sql}")
        tool_context.state["generated_update_sql"] = sql
        return sql

    except Exception as e:
        logging.error(f"Error generating UPDATE SQL: {str(e)}")
        return f"-- Error generating UPDATE SQL: {str(e)}"


def generate_delete_sql(
        request: str,
        tool_context: ToolContext,
) -> str:
    """Generate DELETE SQL from natural language request.

    Args:
        request (str): Natural language description of the DELETE operation
        tool_context (ToolContext): The tool context containing database settings

    Returns:
        str: Generated DELETE SQL statement
    """

    database_settings = tool_context.state.get("database_settings", get_database_settings())
    schema = database_settings.get("bq_ddl_schema", "")

    prompt_template = """
You are a BigQuery SQL expert. Generate a DELETE statement based on the user's request.

**Database Schema:**
{schema}

**User Request:**
{request}

**Guidelines:**
- Generate only the DELETE SQL statement
- Use proper BigQuery syntax and GoogleSQL dialect
- Include specific WHERE clause to limit scope - NEVER create DELETE without WHERE clause
- Be very conservative with WHERE conditions to prevent accidental mass deletions
- Ensure the statement is syntactically correct
- Use proper table references with project.dataset.table format if needed
- Handle date/time formats correctly for BigQuery

**GEOGRAPHY DATA HANDLING:**
- For GEOGRAPHY columns in WHERE clauses, use ST_GEOGPOINT(longitude, latitude) function
- When user provides "latitude X, longitude Y" or "lat X, lng Y", use ST_GEOGPOINT(Y, X)
- When user provides coordinates as "X, Y", assume X is latitude and Y is longitude, so use ST_GEOGPOINT(Y, X)
- ST_GEOGPOINT function signature: ST_GEOGPOINT(longitude_first, latitude_second)
- Example: For latitude 37.7749, longitude -122.4194, use ST_GEOGPOINT(-122.4194, 37.7749)

**SAFETY REQUIREMENT:** Always include a specific WHERE clause. Never generate DELETE statements that would affect all rows.

**Important:** Output only the SQL statement without any markdown formatting, explanations, or additional text.

DELETE SQL:
"""

    logging.info(f"Generating DELETE SQL for request: {request}")

    prompt = prompt_template.format(schema=schema, request=request)

    try:
        response = llm_client.models.generate_content(
            model=os.getenv("DB_OPERATIONS_MODEL", os.getenv("BASELINE_NL2SQL_MODEL", "gemini-2.5-flash")),
            contents=prompt,
            config={"temperature": 0.1},
        )

        sql = response.text
        if sql:
            # Clean up the response
            sql = sql.replace("```sql", "").replace("```", "").strip()
            # Remove any leading explanatory text
            lines = sql.split('\n')
            sql_lines = []
            found_delete = False
            for line in lines:
                if line.strip().upper().startswith('DELETE') or found_delete:
                    found_delete = True
                    sql_lines.append(line)
            sql = '\n'.join(sql_lines).strip()

        logging.info(f"Generated DELETE SQL: {sql}")
        tool_context.state["generated_delete_sql"] = sql
        return sql

    except Exception as e:
        logging.error(f"Error generating DELETE SQL: {str(e)}")
        return f"-- Error generating DELETE SQL: {str(e)}"


def validate_dml_operation(
        sql_statement: str,
        tool_context: ToolContext,
) -> Dict[str, Any]:
    """Validate a DML operation before execution.
    
    Args:
        sql_statement (str): The SQL statement to validate
        tool_context (ToolContext): The tool context
        
    Returns:
        Dict[str, Any]: Validation result with success status and messages
    """

    result = {
        "valid": False,
        "operation_type": None,
        "warnings": [],
        "errors": [],
        "estimated_affected_rows": None
    }

    # Clean up the SQL
    sql_clean = sql_statement.strip().upper()

    # Determine operation type
    if sql_clean.startswith("INSERT"):
        result["operation_type"] = "INSERT"
    elif sql_clean.startswith("UPDATE"):
        result["operation_type"] = "UPDATE"
    elif sql_clean.startswith("DELETE"):
        result["operation_type"] = "DELETE"
    else:
        result["errors"].append("SQL statement must be INSERT, UPDATE, or DELETE")
        return result

    # Check for dangerous patterns
    if result["operation_type"] in ["UPDATE", "DELETE"]:
        if "WHERE" not in sql_clean:
            result["warnings"].append("No WHERE clause detected - this will affect ALL rows!")

        # Check for overly broad conditions
        broad_patterns = [
            r"WHERE\s+1\s*=\s*1",
            r"WHERE\s+TRUE",
            r"WHERE\s+.*\s*>\s*0"
        ]

        for pattern in broad_patterns:
            if re.search(pattern, sql_clean):
                result["warnings"].append("Potentially broad WHERE condition detected")

    # Basic syntax validation using BigQuery dry run
    try:
        client = get_bq_client()
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        client.query(sql_statement, job_config=job_config)
        result["valid"] = True
    except Exception as e:
        result["errors"].append(f"SQL syntax error: {str(e)}")

    return result


def preview_affected_records(
        sql_statement: str,
        tool_context: ToolContext,
) -> Dict[str, Any]:
    """Preview records that would be affected by UPDATE or DELETE operation.

    Args:
        sql_statement (str): The UPDATE or DELETE SQL statement
        tool_context (ToolContext): The tool context

    Returns:
        Dict[str, Any]: Preview of affected records
    """

    result = {
        "success": False,
        "affected_records": [],
        "count": 0,
        "preview_sql": None,
        "error": None
    }

    try:
        # Convert UPDATE/DELETE to SELECT to preview affected records
        sql_upper = sql_statement.strip().upper()

        if sql_upper.startswith("UPDATE"):
            # Extract table and WHERE clause from UPDATE statement
            # UPDATE table SET ... WHERE condition -> SELECT * FROM table WHERE condition
            match = re.search(r'UPDATE\s+([^\s]+).*?WHERE\s+(.+?)(?:\s+LIMIT|\s*$)', sql_statement,
                              re.IGNORECASE | re.DOTALL)
            if match:
                table_name = match.group(1)
                where_clause = match.group(2).strip()
                preview_sql = f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT 100"
            else:
                result["error"] = "Could not parse UPDATE statement for preview"
                return result

        elif sql_upper.startswith("DELETE"):
            # Extract table and WHERE clause from DELETE statement
            # DELETE FROM table WHERE condition -> SELECT * FROM table WHERE condition
            match = re.search(r'DELETE\s+FROM\s+([^\s]+).*?WHERE\s+(.+?)(?:\s+LIMIT|\s*$)', sql_statement,
                              re.IGNORECASE | re.DOTALL)
            if match:
                table_name = match.group(1)
                where_clause = match.group(2).strip()
                preview_sql = f"SELECT * FROM {table_name} WHERE {where_clause} LIMIT 100"
            else:
                result["error"] = "Could not parse DELETE statement for preview"
                return result
        else:
            result["error"] = "Preview only supported for UPDATE and DELETE operations"
            return result

        result["preview_sql"] = preview_sql

        # Execute preview query
        client = get_bq_client()
        query_job = client.query(preview_sql)
        results = query_job.result()

        # Convert results to list of dictionaries
        records = []
        for row in results:
            record = {key: value for key, value in row.items()}
            records.append(record)

        result["affected_records"] = records
        result["count"] = len(records)
        result["success"] = True

        # Also get total count
        count_sql = preview_sql.replace("SELECT *", "SELECT COUNT(*)", 1).replace("LIMIT 100", "")
        count_job = client.query(count_sql)
        count_result = list(count_job.result())[0]
        result["total_count"] = count_result[0]

    except Exception as e:
        result["error"] = f"Error previewing affected records: {str(e)}"

    return result


def execute_dml_operation(
        sql_statement: str,
        tool_context: ToolContext,
        confirmed: bool = False,
) -> Dict[str, Any]:
    """Execute a DML operation (INSERT, UPDATE, DELETE).

    Args:
        sql_statement (str): The SQL statement to execute
        tool_context (ToolContext): The tool context
        confirmed (bool): Whether the user has confirmed the operation

    Returns:
        Dict[str, Any]: Execution result
    """

    result = {
        "success": False,
        "operation_type": None,
        "affected_rows": 0,
        "message": None,
        "error": None,
        "requires_confirmation": False
    }

    # Determine operation type
    sql_upper = sql_statement.strip().upper()
    if sql_upper.startswith("INSERT"):
        result["operation_type"] = "INSERT"
    elif sql_upper.startswith("UPDATE"):
        result["operation_type"] = "UPDATE"
    elif sql_upper.startswith("DELETE"):
        result["operation_type"] = "DELETE"
    else:
        result["error"] = "Only INSERT, UPDATE, and DELETE operations are supported"
        return result

    # For destructive operations, require confirmation
    if result["operation_type"] in ["UPDATE", "DELETE"] and not confirmed:
        result["requires_confirmation"] = True
        result["message"] = f"This {result['operation_type']} operation requires user confirmation before execution."
        return result

    try:
        # Execute the DML operation
        client = get_bq_client()
        query_job = client.query(sql_statement)
        query_job.result()  # Wait for completion

        # Get number of affected rows
        if hasattr(query_job, 'num_dml_affected_rows'):
            result["affected_rows"] = query_job.num_dml_affected_rows

        result["success"] = True
        result[
            "message"] = f"{result['operation_type']} operation completed successfully. {result['affected_rows']} rows affected."

        # Store result in context for potential rollback or further operations
        tool_context.state["last_dml_result"] = result

    except Exception as e:
        result["error"] = f"Error executing {result['operation_type']} operation: {str(e)}"
        result["message"] = f"Failed to execute {result['operation_type']} operation."

    return result
