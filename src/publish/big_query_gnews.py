import json

from google.cloud import bigquery
import dateutil.parser
import logging
import apache_beam as beam
from datetime import datetime
import uuid # Import uuid for generating unique IDs
# No longer strictly needing dateutil.parser if using datetime.utcnow().isoformat()

# Initialize a BigQuery client (ensure this is done once, e.g., at the module level or passed)
client = bigquery.Client()

# Define your table ID
table_id = "schrodingers-cat-466413.vectraCityRaw.LivePulse"

class BigQuerySqlInsertFnGnews(beam.DoFn):
    def process(self, element):
        print(f"ELEMENT REACHED PUBLISH: {element}")
        print(f"XXX: {type(element)}")
        print(f"XXX YYY ZZZ: {json.dumps(element)}")

        data = element

        # Extract latitude and longitude. Handle None and format for SQL.
        latitude_value = data['location'].get('latitude')
        longitude_value = data['location'].get('longitude')
        latitude_for_sql = f"{latitude_value}" if latitude_value is not None else "NULL"
        longitude_for_sql = f"{longitude_value}" if longitude_value is not None else "NULL"

        # --- FIXES TO STRING ESCAPING START HERE ---

        # Correctly escape and assign location and sub_location
        # Use .get() with a default empty string for robustness
        location_for_sql = data['location'].get('area', '').replace("'", "\\'")
        sub_location_for_sql = data['location'].get('sublocation', '').replace("'", "\\'")

        # Prepare category list, ensuring nested names are escaped
        category_list = data.get('problem', []) # Use .get() for safety
        category_bq_format = []
        for cat_item in category_list:
            cat_name = cat_item.get('category')
            if cat_name is None:
                continue
            # Correctly escape and assign back
            cat_name_escaped = cat_name.replace("'", "\\'")
            category_bq_format.append(f"STRUCT('{cat_name_escaped}' AS name, {cat_item.get('relevancy_score', 0.0)} AS relevancy)")
        category_bq_string = f"[{', '.join(category_bq_format)}]" if category_bq_format else "[]"

        # Prepare sub_category list, ensuring nested names are escaped
        sub_category_list = []
        for problem_cat in category_list: # Iterate through problem_cat to get subcategories
            if 'subcategory' in problem_cat and problem_cat['subcategory']:
                sub_category_list.extend(problem_cat['subcategory'])

        sub_category_bq_format = []
        for sub_cat_item in sub_category_list:
            sub_cat_name = sub_cat_item.get('category')
            if sub_cat_name is None:
                continue
            # Correctly escape and assign back
            sub_cat_name_escaped = sub_cat_name.replace("'", "\\'")
            sub_category_bq_format.append(f"STRUCT('{sub_cat_name_escaped}' AS name, {sub_cat_item.get('relevancy_score', 0.0)} AS relevancy)")
        sub_category_bq_string = f"[{', '.join(sub_category_bq_format)}]" if sub_category_bq_format else "[]"

        # Source: Your SQL uses 'google_news' literal. If 'extracted_data['source']' was used:
        # source_for_sql = extracted_data.get('source', '').replace("'", "\\'") # Example if dynamic

        # AI analysis (summary): Correctly escape and assign
        ai_analysis_for_sql = data.get('summary', '').replace("'", "\\'")

        # Prepare department list, ensuring nested names are escaped
        department_bq_format = []
        for dept_item in data.get('department', []): # Use .get() for safety
            dept_name = dept_item.get('department')
            if dept_name is None:
                continue
            # Correctly escape and assign back
            dept_name_escaped = dept_name.replace("'", "\\'")
            department_bq_format.append(f"STRUCT('{dept_name_escaped}' AS name, {dept_item.get('relevancy_score', 0.0)} AS relevancy)")
        department_bq_string = f"[{', '.join(department_bq_format)}]" if department_bq_format else "[]"

        # --- FIXES TO STRING ESCAPING END HERE ---


        # Severity logic (unchanged)
        max_problem_relevancy = 0
        for problem_cat in data.get('problem', []):
            max_problem_relevancy = max(max_problem_relevancy, problem_cat.get('relevancy_score', 0.0))
            if 'subcategory' in problem_cat:
                for sub_cat_item in problem_cat.get('subcategory', []):
                    max_problem_relevancy = max(max_problem_relevancy, sub_cat_item.get('relevancy_score', 0.0))

        record_id = str(uuid.uuid4())
        record_time_iso = datetime.utcnow().isoformat(timespec='microseconds')
        severity = 'P3'

        print(f"Generated Record ID: {record_id}")
        # Removed print(extracted_data) as it might contain unescaped versions
        # and cause confusion if debugging SQL errors.

        sql_insert_statement = f"""
        INSERT INTO {table_id} (
            id,
            record_time,
            latitude,
            longitude,
            location,
            sub_location,
            category,
            sub_category,
            source,
            ai_analysis,
            department,
            severity
        )
        VALUES (
            '{record_id}',
            TIMESTAMP'{record_time_iso}',
            {latitude_for_sql},
            {longitude_for_sql},
            '{location_for_sql}',     -- Use the correctly escaped string
            '{sub_location_for_sql}', -- Use the correctly escaped string
            {category_bq_string},
            {sub_category_bq_string},
            'google_news',
            '{ai_analysis_for_sql}',  -- Use the correctly escaped string
            {department_bq_string},
            '{severity}'
        );
        """

        try:
            query_job = client.query(sql_insert_statement)
            query_job.result()
            print(f"Data successfully inserted into {table_id}.")
            print(f"Job ID: {query_job.job_id}")
            print(f"Rows affected: {query_job.num_dml_affected_rows}")

        except Exception as e:
            logging.error(f"An error occurred during BigQuery insert: {e}")
            logging.error(f"Failed SQL: {sql_insert_statement}")
            raise