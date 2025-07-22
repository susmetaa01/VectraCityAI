import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
import os
import json # Trigger events will be JSON strings

# --- Configuration for Google Cloud Project and Pub/Sub ---
# Set these as environment variables or pass via pipeline options for production
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'schrodingers-cat-466413') # Replace with your actual project ID
PUBSUB_SUBSCRIPTION_NAME = os.environ.get('PUBSUB_SUBSCRIPTION_NAME', 'vectraCityAI-event-trigger-sub')

# Construct the full Pub/Sub subscription path
# It should look like projects/your-gcp-project-id/subscriptions/your-subscription-name
PUBSUB_SUBSCRIPTION_PATH = f"projects/{PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_NAME}"

# --- Main Pipeline Definition ---
def run_pipeline():
    # Set pipeline options for DataflowRunner (for deployment) or DirectRunner (for local testing)
    pipeline_options = PipelineOptions()

    # Configure for DataflowRunner if deploying to GCP
    # For local testing, you'd typically run with --runner=DirectRunner
    # When deploying to Dataflow, these options are crucial:
    # pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner' # Or 'DirectRunner' for local
    # pipeline_options.view_as(GoogleCloudOptions).project = PROJECT_ID
    # pipeline_options.view_as(GoogleCloudOptions).region = 'south-asia1' # Or your preferred Dataflow region
    # pipeline_options.view_as(GoogleCloudOptions).temp_location = f'gs://{PROJECT_ID}-dataflow-temp/tmp' # Ensure GCS bucket exists
    # pipeline_options.view_as(GoogleCloudOptions).staging_location = f'gs://{PROJECT_ID}-dataflow-temp/staging' # Ensure GCS bucket exists
    # Add other Dataflow options like service_account_email, max_num_workers, etc., if needed

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read messages from the specified Pub/Sub subscription
        # These messages are expected to be bytes, containing a JSON string.
        trigger_events = (
                pipeline
                | 'ReadFromTriggerPubSub' >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION_PATH)
        )

        # Decode the bytes and parse the JSON payload
        parsed_events = (
                trigger_events
                | 'DecodeAndParseJson' >> beam.Map(lambda element: json.loads(element.decode('utf-8')))
        )

        # Print the received and parsed payload to standard output
        # In a real pipeline, this would be passed to further processing steps (e.g., AI comprehension, database writes)
        parsed_events | 'PrintPayload' >> beam.Map(print)

# --- Main execution block ---
if __name__ == '__main__':
    # Set environment variables for local testing (replace with your actual values)
    # export GCP_PROJECT_ID='schrodingers-cat-466413'
    # export PUBSUB_SUBSCRIPTION_NAME='vectraCityAI-event-trigger-sub'

    print("Starting Apache Beam pipeline to listen for trigger events...")
    run_pipeline()
    print("Apache Beam pipeline finished.") # This message might not print immediately for streaming pipelines