import os

# --- Google Cloud Project Configuration ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'schrodingers-cat-466413')
# Updated region to 'asia-south1' as Chennai (your current location) is nearby
DATAFLOW_REGION = os.getenv('DATAFLOW_REGION', 'asia-south1')

# --- Pub/Sub Subscriptions ---
# Subscription for WhatsApp trigger events (from your Flask app)
PUBSUB_SUBSCRIPTION_NAME_TRIGGER = os.getenv('PUBSUB_SUBSCRIPTION_NAME_TRIGGER', 'vectraCityAI-event-trigger-sub')
# Subscription for raw Twitter feed data (from your Twitter ingestion script)
PUBSUB_SUBSCRIPTION_NAME_TWITTER = os.getenv('PUBSUB_SUBSCRIPTION_NAME_TWITTER', 'twitter-incoming-raw-events-sub')
PUBSUB_SUBSCRIPTION_NAME_GNEWS = os.getenv('PUBSUB_SUBSCRIPTION_NAME_GNEWS', 'gnews-incoming-raw-events-sub')

# --- Google Cloud Storage for Dataflow staging/temp files ---
# Ensure this bucket exists in your GCP project and is accessible by your Dataflow service account
GCS_DATAFLOW_BUCKET = os.getenv('GCS_DATAFLOW_BUCKET', 'vectracityai_events')
GCS_TEMP_LOCATION = f'gs://{GCS_DATAFLOW_BUCKET}/tmp'
GCS_STAGING_LOCATION = f'gs://{GCS_DATAFLOW_BUCKET}/staging'

# --- Other configurations (add as needed for InfluxDB, Google Maps API keys, etc.) ---
# Example for InfluxDB (if you'll write to it from Beam)
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "your-influxdb-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "your-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "your-bucket")