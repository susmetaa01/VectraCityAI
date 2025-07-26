from google.cloud import bigquery
import logging
import apache_beam as beam
from datetime import datetime
import uuid # Import uuid for generating unique IDs

# Initialize a BigQuery client
client = bigquery.Client()

# Define your table ID
# Project ID, Dataset ID, Table ID
table_id = "schrodingers-cat-466413.vectraCityRaw.LivePulse"

class BigQuerySqlInsertFn(beam.DoFn):

    def process(self, element):
        print(f"ELEMENT REACHED PUBLISH: {element}")

        # Extract fields from the element dictionary
        # Generate a unique ID for each record
        record_id = str(uuid.uuid4())
        record_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f UTC')[:-3] + ' UTC' # Current UTC time

        # element.get('text') = element.get('text')

        latitude = element.get('text').get('location', {}).get('latitude')
        longitude = element.get('text').get('location', {}).get('longitude')
        location = element.get('text').get('location', {}).get('area')
        sub_location = element.get('text').get('location', {}).get('sublocation')

        # Category and Sub-category from AI analysis, defaulting to empty list if not present
        category_list = element.get('text').get('ai_analysis', {}).get('category', [])
        sub_category_list = element.get('text').get('ai_analysis', {}).get('sub_category', [])

        # Format category and sub_category as BQ STRUCT array strings
        category_bq_format = []
        for cat in category_list:
            category_bq_format.append(f"STRUCT('{cat['category']}' AS name, {cat['relevancy_score']} AS relevancy)")
        category_bq_string = f"[{', '.join(category_bq_format)}]" if category_bq_format else "[]"

        sub_category_bq_format = []
        for sub_cat in sub_category_list:
            sub_category_bq_format.append(f"STRUCT('{sub_cat['sub_category']}' AS name, {sub_cat['relevancy_score']} AS relevancy)")
        sub_category_bq_string = f"[{', '.join(sub_category_bq_format)}]" if sub_category_bq_format else "[]"


        source = element.get('source', 'Unknown') # Assuming 'source' might be at the top level
        ai_analysis_summary = element.get('summary', '') # Use 'summary' as ai_analysis

        # Department from the 'department' list, defaulting to empty list if not present
        department_list = element.get('department', [])
        department_bq_format = []
        for dept in department_list:
            department_bq_format.append(f"STRUCT('{dept['department']}' AS name, {dept['relevancy_score']} AS relevancy)")
        department_bq_string = f"[{', '.join(department_bq_format)}]" if department_bq_format else "[]"


        # Severity is not directly available in the sample, defaulting to a placeholder
        # You'll need to determine how 'severity' is derived in your actual pipeline
        severity = element.get('severity', 'P3') # Defaulting to 'P3' as it's not in sample

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
            '{record_time}',
            {latitude},
            {longitude},
            '{location}',
            '{sub_location}',
            {category_bq_string},
            {sub_category_bq_string},
            '{source}',
            '{ai_analysis_summary}',
            {department_bq_string},
            '{severity}'
        );
        """

        try:
            # Run the query
            query_job = client.query(sql_insert_statement)

            # Wait for the query to complete
            query_job.result()

            print(f"Data successfully inserted into {table_id}.")
            print(f"Job ID: {query_job.job_id}")
            print(f"Rows affected: {query_job.num_dml_affected_rows}")

        except Exception as e:
            logging.error(f"An error occurred during BigQuery insert: {e}")
            logging.error(f"Failed SQL: {sql_insert_statement}") # Log the failed SQL for debugging
            raise # Re-raise the exception to propagate the error in the pipeline