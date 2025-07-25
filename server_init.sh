echo "Setting up environment variables for VectraCityAI project..."

# --- 1. Activate Python Virtual Environment ---
# Ensure your virtual environment is created at venv/
if [ -d "venv" ]; then
    source VectraCityAIEnv/bin/activate
    echo "Virtual environment 'venv' activated."
else
    echo "Error: Virtual environment 'venv' not found. Please create it first (python3 -m venv venv)."
    exit 1
fi

# --- 2. Google Cloud Project Configuration ---
export GCP_PROJECT_ID="schrodingers-cat-466413"
export DATAFLOW_REGION="asia-south1" # Make sure this matches your Firestore region and is a Dataflow-supported region

# --- 3. Pub/Sub Topics & Subscriptions ---
# Main topic for full, enriched message payloads (consumed by Beam pipeline)
export PUBSUB_TOPIC_ID_INCOMING="whatsapp-incoming-raw-events"
# Topic for just SIDs, acting as a signal/trigger for downstream systems
export PUBSUB_TOPIC_ID_TRIGGER="vectraCityAI-event-triggers" # Corrected name from PUB_SUB_TOPIC_ID_TRIGGER
# Subscription for WhatsApp trigger events (consumed by Beam pipeline's trigger branch)
export PUBSUB_SUBSCRIPTION_NAME_TRIGGER="vectraCityAI-event-trigger-sub"
# Subscription for raw Twitter feed data (consumed by Beam pipeline's Twitter branch)
export PUBSUB_SUBSCRIPTION_NAME_TWITTER="twitter-incoming-raw-events-sub"
export PUBSUB_TOPIC_GNEWS_RAW_NEWS="gnews-incoming-raw-events-sub"

export GEMINI_API_KEY="AIzaSyAoje8vVTrVcqLCwGtx9lVyNzMPXFRyYPw"
export GOOGLE_MAPS_API_KEY="AIzaSyAJ7r-hVzCqkQ9mPq_DHDVTiDSRUCcFK74"




# --- 4. Google Cloud Storage Buckets ---
# GCS Bucket for media files uploaded from WhatsApp (and temp media storage)
export GCS_MEDIA_BUCKET="vectra_city_ai-whatsapp-media"
# GCS Bucket for Dataflow staging/temp files
export GCS_DATAFLOW_BUCKET="${GCP_PROJECT_ID}-dataflow-temp" # Derived from project ID as in config.py


# --- 5. Twilio API Credentials ---
# !!! REPLACE WITH YOUR ACTUAL TWILIO CREDENTIALS !!!
export TWILIO_ACCOUNT_SID="AC95b60f8ee2f0ed253bd27cd4a50d9dfa"
export TWILIO_AUTH_TOKEN="6215f2f4af9091e8ff996c791a36f0eb"
export TWILIO_WHATSAPP_NUMBER="whatsapp:+14155238886"


# --- 6. Twitter API Credentials (for ingestors/twitter_stream_publisher.py) ---
export TWITTER_BEARER_TOKEN="AAAAAAAAAAAAAAAAAAAAANjK3AEAAAAAcbM1cUJO7BwCR1SubASDUYoe4gM%3DW1vPLXdAO4vBgL7jtywxWjgxiBfeU0U05LnvBTm4U5eHMh6rxB"
export API_KEY_SECRET="PmrtvN6YGlbXpYQhiRVq8070NXqh1zbJQy5j2nOZFGoUgjUy7s"
export API_KEY="dymug9tC31816CgvgkBkgvsRc"


# --- 7. InfluxDB Configuration (if used directly by any component, like Beam) ---
#TODO: fill in the influx Creds
#export INFLUXDB_URL="http://localhost:8086"
#export INFLUXDB_TOKEN="your-influxdb-token"
#export INFLUXDB_ORG="your-org"
#export INFLUXDB_BUCKET="your-bucket"


echo "Environment variables set. You can now run your project components."
echo "--- COMMAND EXAMPLES ---"
echo "To run Flask App (WhatsApp Webhook Listener):"
echo "  python app/whatsapp_bot_firestore.py"
echo ""
echo "To run Beam Pipeline (Local - DirectRunner):"
echo "  python -m src.vectra_city_pipeline --runner=DirectRunner --streaming"
echo ""
echo "To run Twitter Ingestor:"
echo "  python src/ingestor/twitter_stream_publisher.py"
echo ""
echo "To deploy Beam Pipeline to Dataflow (example, requires GCS bucket for temp/staging):"
echo "  python -m src.vectra_city_pipeline \\"
echo "    --runner=DataflowRunner \\"
echo "    --project=\$GCP_PROJECT_ID \\"
echo "    --region=\$DATAFLOW_REGION \\"
echo "    --temp_location=gs://\$GCS_DATAFLOW_BUCKET/tmp \\"
echo "    --staging_location=gs://\$GCS_DATAFLOW_BUCKET/staging \\"
echo "    --streaming \\"
echo "    --job_name=unified-city-pulse-\$(date +%Y%m%d%H%M%S)"
echo "------------------------"

# Optional: You can uncomment one of the lines below to automatically start a component
# For example, to start the Flask app immediately after setup:
# python app/whatsapp_bot_firestore.py

# Or to start the Beam pipeline:
# python -m src.vectra_city_pipeline --runner=DirectRunner --streaming

# Or to start the Twitter ingestor:
# python ingestors/twitter_stream_publisher.py