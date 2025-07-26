import json

from google.cloud import bigquery
import dateutil.parser
import logging
import apache_beam as beam
from datetime import datetime
import uuid # Import uuid for generating unique IDs
import dateutil.parser # For robust date parsing

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


        extracted_data = {}

        extracted_data['latitude'] = data['location']['latitude']
        extracted_data['longitude'] = data['location']['longitude']

        # Extract location and sub_location
        extracted_data['location'] = data['location']['area']
        extracted_data['sub_location'] = data['location']['sublocation']

        category_list = data['problem']
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

        # Source and AI analysis are not present in the given dictionary, so setting them as None or empty string
        extracted_data['source'] = None # Or an empty string: ''
        extracted_data['ai_analysis'] = data['summary'] # The 'summary' field can be considered as AI analysis or a description

        # Extract department
        # Assuming we want a list of departments
        department_bq_format = []
        for dept in data['department']:
            department_bq_format.append(f"STRUCT('{dept.get('department')}' AS name, {dept.get('relevancy_score')} AS relevancy)")
        department_bq_string = f"[{', '.join(department_bq_format)}]" if department_bq_format else "[]"
        extracted_data['department'] = department_bq_string

        # Severity is not directly provided as a single field.
        # We can infer it from relevancy scores of problems, or if there's a specific 'severity' field.
        # For now, let's derive a simple severity based on the highest relevancy score in problems.
        # This is an example of how you might infer it. You might need a more sophisticated logic.
        max_problem_relevancy = 0
        for problem_cat in data['problem']:
            max_problem_relevancy = max(max_problem_relevancy, problem_cat['relevancy_score'])
            for sub_cat in problem_cat['subcategory']:
                max_problem_relevancy = max(max_problem_relevancy, sub_cat['relevancy_score'])


        # The 'id' field is not present in the provided dictionary. You would typically generate this
        # when storing the data, e.g., using a UUID or an auto-incrementing ID from a database.
        extracted_data['id'] = str(uuid.uuid4())
        extracted_data['record_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f UTC')[:-3] + ' UTC'


        event_timestamp_iso = datetime.now().isoformat()
        severity = 'P3'

        print(extracted_data)


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
            '{extracted_data.get('record_id')}',
            TIMESTAMP'{event_timestamp_iso}',
            'NULL',
            'NULL',
            '{extracted_data.get('location')}',
            '{extracted_data.get('sub_location')}',
            {category_bq_string},
            {sub_category_bq_string},
            '{extracted_data.get('source')}',
            '{extracted_data.get('ai_analysis_summary')}',
            '{department_bq_string}',
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
