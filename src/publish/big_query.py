import logging
from google.cloud import bigquery
import apache_beam as beam

_LOGGER = logging.getLogger(__name__)

class BigQuerySqlInsertFn(beam.DoFn):
    """
    A DoFn that constructs and executes a SQL INSERT statement for each element.
    This is generally NOT RECOMMENDED for high-throughput streaming pipelines
    due to performance, scalability, and error handling complexities compared
    to beam.io.WriteToBigQuery.
    """
    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = None
        self.full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    def setup(self):
        # Initialize the BigQuery client. This should happen once per worker.
        self.client = bigquery.Client(project=self.project_id)
        _LOGGER.info(f"BigQuery client initialized for project: {self.project_id}")

    def process(self, element):
        """
        Constructs and executes an INSERT statement for the given element.
        Assumes 'element' is a dictionary matching the BigQuery schema.
        """
        try:
            # Safely get values, providing defaults for NULLABLE fields if not present
            # Ensure your pipeline's output dictionary matches this structure
            _id = element.get('id')
            record_time = element.get('record_time')
            latitude = element.get('latitude')
            longitude = element.get('longitude')
            location = element.get('location')
            sub_location = element.get('sub_location')
            category = element.get('category', []) # category is REPEATED RECORD
            _name = element.get('name') # Top-level name
            _relevancy = element.get('relevancy') # Top-level relevancy
            sub_category = element.get('sub_category', []) # sub_category is REPEATED RECORD
            source = element.get('source')
            ai_analysis = element.get('ai_analysis')
            department = element.get('department', []) # department is REPEATED RECORD

            # Validate REQUIRED fields before proceeding
            required_fields = {
                'id': _id, 'record_time': record_time, 'latitude': latitude, 'longitude': longitude,
                'location': location, 'name': _name, 'relevancy': _relevancy,
                'source': source, 'ai_analysis': ai_analysis
            }
            for field_name, value in required_fields.items():
                if value is None:
                    _LOGGER.error(f"Missing REQUIRED field: {field_name} for element: {element}")
                    return # Skip this element or send to dead-letter queue

            # Format REPEATED RECORD fields into SQL array literals of STRUCTs
            # Example: [STRUCT('Road' AS name, 0.95 AS relevancy)]
            formatted_category = []
            for cat_item in category:
                if 'name' in cat_item and 'relevancy' in cat_item:
                    formatted_category.append(
                        f"STRUCT('{cat_item['name'].replace(\"'\", \"\\\'\")}' AS name, {float(cat_item['relevancy'])} AS relevancy)"
                    )
            category_sql = f"[{','.join(formatted_category)}]" if formatted_category else "[]"

                    formatted_sub_category = []
                    for sub_cat_item in sub_category:
                    name_val = f"'{sub_cat_item['name'].replace(\"'\", \"\\\'\")}'" if 'name' in sub_cat_item and sub_cat_item['name'] is not None else 'NULL'
                    relevancy_val = f"{float(sub_cat_item['relevancy'])}" if 'relevancy' in sub_cat_item and sub_cat_item['relevancy'] is not None else 'NULL'
                    formatted_sub_category.append(
                    f"STRUCT({name_val} AS name, {relevancy_val} AS relevancy)"
                    )
                    sub_category_sql = f"[{','.join(formatted_sub_category)}]" if formatted_sub_category else "[]"


                    formatted_department = []
                    for dept_item in department:
                        if 'name' in dept_item and 'relevancy' in dept_item:
                    formatted_department.append(
                        f"STRUCT('{dept_item['name'].replace(\"'\", \"\\\'\")}' AS name, {float(dept_item['relevancy'])} AS relevancy)"
                    )
            department_sql = f"[{','.join(formatted_department)}]" if formatted_department else "[]"


                    # Handle NULLABLE fields
                    sub_location_sql = f"'{sub_location.replace(\"'\", \"\\\'\")}'" if sub_location is not None else 'NULL'

            # Construct the SQL INSERT statement
            # Use parameterized queries for actual production to prevent SQL injection and for better performance
            # However, for a simple per-row insert, string formatting is shown here as requested.
            sql_insert_statement = f"""
            INSERT INTO `{self.full_table_id}` (
                id,
                record_time,
                geography,
                location,
                sub_location,
                category,
                name,
                relevancy,
                sub_category,
                source,
                ai_analysis,
                department
            )
            VALUES (
                '{_id}',
                '{record_time}',
                ST_GEOGPOINT({longitude}, {latitude}),
                '{location.replace("'", "\\'")}',
                {sub_location_sql},
                {category_sql},
                '{_name.replace("'", "\\'")}',
                {float(_relevancy)},
                {sub_category_sql},
                '{source.replace("'", "\\'")}',
                '{ai_analysis.replace("'", "\\'")}',
                {department_sql}
            );
            """

                    query_job = self.client.query(sql_insert_statement)
                    query_job.result() # Wait for the query to complete
                    _LOGGER.info(f"Successfully inserted record with id: {_id}. Rows affected: {query_job.num_dml_affected_rows}")

        except Exception as e:
            _LOGGER.error(f"Error inserting record: {element}. Error: {e}", exc_info=True)
            # In a real pipeline, you would yield the failed element to a dead-letter queue
            # yield pvalue.TaggedOutput('failed_inserts', element)