from google.cloud import bigquery
import logging
import apache_beam as beam
from datetime import datetime
import uuid # Import uuid for generating unique IDs
import dateutil.parser # For robust date parsing

# Initialize a BigQuery client (ensure this is done once, e.g., at the module level or passed)
client = bigquery.Client()

# Define your table ID
table_id = "schrodingers-cat-466413.vectraCityRaw.LivePulse"

class BigQuerySqlInsertFn(beam.DoFn):
    def process(self, element):
        print(f"ELEMENT REACHED PUBLISH: {element}")

        # This BigQuerySqlInsertFn expects an element that has already undergone AI analysis
        # and has a 'parsed' attribute (e.g., a google.generativeai.types.GenerateContentResponse).
        # For a Google News RSS payload, you'd call process_google_news_rss *before* this DoFn,
        # or have a separate pipeline branch for it.
        if not hasattr(element, 'parsed') or element.parsed is None:
            logging.error(f"Element received does not have a 'parsed' attribute or it is None: {element}")
            # Decide how to handle this: skip, raise error, or log to a dead-letter queue.
            raise ValueError("Invalid element received: 'parsed' attribute missing or None. This DoFn expects pre-analyzed data.")

        parsed_data = element.parsed.model_dump()

        record_id = str(uuid.uuid4())
        # Use the published_date from the AI analysis if available, otherwise current UTC
        record_time = parsed_data.get('published_date') or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f UTC')[:-3] + ' UTC'

        event_timestamp_iso = None
        if record_time:
            try:
                event_timestamp_iso = dateutil.parser.parse(record_time).isoformat()
            except Exception:
                logging.warning(f"Could not parse timestamp '{record_time}'. Falling back to now.")
                event_timestamp_iso = datetime.utcnow().isoformat()

        latitude = parsed_data.get('location', {}).get('latitude')
        longitude = parsed_data.get('location', {}).get('longitude')
        location = parsed_data.get('location', {}).get('area')
        sub_location = parsed_data.get('location', {}).get('sublocation')

        category_list = parsed_data.get('problem', [])
        sub_category_list = []
        for problem_cat in category_list:
            if 'subcategory' in problem_cat:
                sub_category_list.extend(problem_cat['subcategory'])

        category_bq_format = []
        for cat in category_list:
            category_bq_format.append(f"STRUCT('{cat.get('category')}' AS name, {cat.get('relevancy_score')} AS relevancy)")
        category_bq_string = f"[{', '.join(category_bq_format)}]" if category_bq_format else "[]"

        sub_category_bq_format = []
        for sub_cat in sub_category_list:
            sub_category_bq_format.append(f"STRUCT('{sub_cat.get('category')}' AS name, {sub_cat.get('relevancy_score')} AS relevancy)")
        sub_category_bq_string = f"[{', '.join(sub_category_bq_format)}]" if sub_category_bq_format else "[]"

        source = parsed_data.get('source', 'Unknown') # Get source from parsed data, or default
        ai_analysis_summary = parsed_data.get('summary', '')

        department_list = parsed_data.get('department', [])
        department_bq_format = []
        for dept in department_list:
            department_bq_format.append(f"STRUCT('{dept.get('department')}' AS name, {dept.get('relevancy_score')} AS relevancy)")
        department_bq_string = f"[{', '.join(department_bq_format)}]" if department_bq_format else "[]"

        severity = parsed_data.get('severity', 'P3')

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
            TIMESTAMP'{event_timestamp_iso}',
            {latitude if latitude is not None else 'NULL'},
            {longitude if longitude is not None else 'NULL'},
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
            query_job = client.query(sql_insert_statement)
            query_job.result()
            print(f"Data successfully inserted into {table_id}.")
            print(f"Job ID: {query_job.job_id}")
            print(f"Rows affected: {query_job.num_dml_affected_rows}")

        except Exception as e:
            logging.error(f"An error occurred during BigQuery insert: {e}")
            logging.error(f"Failed SQL: {sql_insert_statement}")
            raise
