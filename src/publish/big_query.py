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

        # The 'element' is now a google.generativeai.types.GenerateContentResponse object.
        # We need to access its 'parsed' attribute, which contains the AnalysisResponse Pydantic model.
        # The Pydantic model can be converted to a dictionary using .model_dump() for easier access.

        if not hasattr(element, 'parsed') or element.parsed is None:
            logging.error(f"Element received does not have a 'parsed' attribute or it is None: {element}")
            # Decide how to handle this: skip, raise error, or log to a dead-letter queue.
            # For now, we'll raise an error to clearly show the data flow issue.
            raise ValueError("Invalid element received: 'parsed' attribute missing or None.")

        # Convert the parsed Pydantic model to a dictionary
        # The 'parsed' attribute itself is an AnalysisResponse Pydantic model instance.
        # We need to call .model_dump() (or .dict() for older Pydantic) on it.
        # Ensure the AnalysisResponse Pydantic model (from your 'model' package) has these attributes.
        parsed_data = element.parsed.model_dump() # Or element.parsed.dict() if you are on Pydantic v1.x

        # Extract fields from the 'parsed_data' dictionary (which is what you originally expected)
        record_id = str(uuid.uuid4())
        record_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f UTC')[:-3] + ' UTC' # Current UTC time

        if record_time:
            try:
                import dateutil.parser
                event_timestamp_iso = dateutil.parser.parse(record_time).isoformat()
            except Exception:
                logging.warning(f"Could not parse timestamp '{event_timestamp_iso}' for event. Falling back to now.")
                event_timestamp_iso = datetime.utcnow().isoformat()

        # Corrected access: directly from parsed_data
        # Note: You were trying element.get('text').get('location') which was incorrect.
        # The location is directly under the top-level parsed_data dictionary.
        latitude = parsed_data.get('location', {}).get('latitude')
        longitude = parsed_data.get('location', {}).get('longitude')
        location = parsed_data.get('location', {}).get('area')
        sub_location = parsed_data.get('location', {}).get('sublocation')

        # Category and Sub-category from AI analysis
        category_list = parsed_data.get('problem', []) # 'problem' key holds the category/subcategory list
        sub_category_list = []
        for problem_cat in category_list:
            if 'subcategory' in problem_cat:
                sub_category_list.extend(problem_cat['subcategory'])

        # Format category and sub_category as BQ STRUCT array strings
        category_bq_format = []
        for cat in category_list:
            category_bq_format.append(f"STRUCT('{cat.get('category')}' AS name, {cat.get('relevancy_score')} AS relevancy)")
        category_bq_string = f"[{', '.join(category_bq_format)}]" if category_bq_format else "[]"

        sub_category_bq_format = []
        for sub_cat in sub_category_list:
            sub_category_bq_format.append(f"STRUCT('{sub_cat.get('category')}' AS name, {sub_cat.get('relevancy_score')} AS relevancy)")
        sub_category_bq_string = f"[{', '.join(sub_category_bq_format)}]" if sub_category_bq_format else "[]"

        # The source would typically come from the *original* element before AI analysis.
        # Since you're passing only the AI response now, you might need to carry the source
        # through in your AI comprehension step, or derive it.
        # For now, let's assume it's still available or set a default.
        # If the original element (before AI analysis) also had a 'source' key, you'd need
        # to ensure that AIComprehensionFn passes both the AI response and the original metadata.
        # For this fix, let's just make 'source' a default or re-evaluate its origin.
        # Based on previous context, 'source' was a top-level key in the element before AI analysis.
        # If AIComprehensionFn only yields the Gemini response, this info is lost.
        # Re-evaluate your AIComprehensionFn to potentially yield a tuple or dict like:
        # yield {'gemini_response': gemini_response_obj, 'original_source_type': original_element.get('source')}
        # For now, let's hardcode or make it 'Unknown' if not explicitly passed from upstream.
        source = 'WhatsApp' # Assuming it's from WhatsApp stream, or pass it explicitly.
        # If you need the *original* source from the input before AI analysis,
        # you must modify AIComprehensionFn to yield it along with the AI response.

        ai_analysis_summary = parsed_data.get('summary', '') # Use 'summary' from parsed_data

        # Department from the 'department' list
        department_list = parsed_data.get('department', [])
        department_bq_format = []
        for dept in department_list:
            department_bq_format.append(f"STRUCT('{dept.get('department')}' AS name, {dept.get('relevancy_score')} AS relevancy)")
        department_bq_string = f"[{', '.join(department_bq_format)}]" if department_bq_format else "[]"

        # Severity is not directly available in the sample, defaulting to a placeholder
        severity = parsed_data.get('severity', 'P3') # Defaulting to 'P3' if not in parsed_data

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