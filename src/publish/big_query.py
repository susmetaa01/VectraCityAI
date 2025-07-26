from google.cloud import bigquery
import logging
import apache_beam as beam

# Initialize a BigQuery client
client = bigquery.Client()

# Define your table ID
# Project ID, Dataset ID, Table ID
table_id = "schrodingers-cat-466413.vectraCityRaw.LivePulse"

class BigQuerySqlInsertFn(beam.DoFn):

    def process(self, element):

        print(f"ELEMENT REACHED PUBLISH: {element}")

        sql_insert_statement = """
        INSERT INTO {} (
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
            'test_pulse1',
            '2025-07-26 10:30:00.12 UTC',
            -74.0060,
            -40.7128,
            'New York City',
            'Manhattan',
            [STRUCT('Road' AS name, 0.95 AS relevancy)],
            [],
            'Waze_Report',
            'Heavy traffic detected on major arteries due to an unexpected event. Expected delays of 30-45 minutes.',
            [STRUCT('Municipality' AS name, 0.87 AS relevancy)],
            'P1'
        );
        """.format(table_id)

        try:
            # Run the query
            query_job = client.query(sql_insert_statement)

            # Wait for the query to complete
            query_job.result()

            print(f"Data successfully inserted into {table_id}.")
            print(f"Job ID: {query_job.job_id}")
            print(f"Rows affected: {query_job.num_dml_affected_rows}")

        except Exception as e:
            print(f"An error occurred: {e}")