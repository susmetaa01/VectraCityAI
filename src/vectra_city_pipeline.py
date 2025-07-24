import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
import os

# Relative imports from your newly created 'src' package
from . import config
from . import io_connectors
from . import transform

def run_pipeline():
    """
    Main function to define and run the Apache Beam pipeline.
    This pipeline reads from WhatsApp trigger events and raw Twitter feeds,
    and prints the parsed payloads.
    """
    pipeline_options = PipelineOptions()

    # Configure common pipeline options for Dataflow deployment
    # pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner' # Use 'DirectRunner' for local testing
    pipeline_options.view_as(GoogleCloudOptions).project = config.GCP_PROJECT_ID
    pipeline_options.view_as(GoogleCloudOptions).region = config.DATAFLOW_REGION
    pipeline_options.view_as(GoogleCloudOptions).temp_location = config.GCS_TEMP_LOCATION
    pipeline_options.view_as(GoogleCloudOptions).staging_location = config.GCS_STAGING_LOCATION
    pipeline_options.view_as(StandardOptions).streaming = True # Essential for Pub/Sub sources

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # --- Branch 1: WhatsApp Trigger Events ---
        # This branch reads signals that a WhatsApp event has been fully processed and stored (with location)
        whatsapp_trigger_events_pcoll = (
                pipeline
                | 'ReadWhatsAppTriggerEvents' >> io_connectors.ReadTriggerEvents() # Uses PTransform from io_connectors.py
                | 'PrintWhatsAppTriggerPayload' >> beam.Map(lambda x: print(f"WhatsApp Trigger Event: {x}")) # For debugging/logging
        )

        # --- Branch 2: Raw Twitter Feed ---
        # This branch reads raw tweets published from your external Twitter ingestion script
        # raw_twitter_feed_pcoll = (
        #         pipeline
        #         | 'ReadRawTwitterFeed' >> io_connectors.ReadTwitterFeed() # Uses PTransform from io_connectors.py
        #         | 'ParseTwitterTweetData' >> beam.ParDo(transform.twitter_feed_transformer.ParseTweetFn()) # Uses DoFn from twitter_feed_transformer.py
        #         | 'PrintParsedTweet' >> beam.Map(lambda x: print(f"Parsed Twitter Tweet: {x}")) # For debugging/logging
        # )

        raw_google_news_feed_data = (
                pipeline
                | 'ReadRawGoogleNewsFeed' >> io_connectors.ReadGoogleNewsFeed() # Use PTransform
                | 'DecodeAndParseNewsJson' >> beam.Map(lambda element: json.loads(element.decode('utf-8'))) # News already parsed to JSON
                | 'PrintParsedNewsArticle' >> beam.Map(lambda x: print(f"Parsed News Article: {x}")) # For debugging/logging
        )

        # --- Future: Unified Processing and AI Comprehension ---
        # In a complete project, you would:
        # 1. Read the *full* WhatsApp event payloads (not just triggers) from another Pub/Sub topic
        #    (e.g., 'whatsapp-full-events-data-sub' which is where the Flask app publishes the enriched payload).
        # 2. Apply AI Comprehension (Gemini NLP, Vertex AI Vision/Video) to both parsed tweets and processed WhatsApp events.
        # 3. Geocode any implicit locations (e.g., from tweet text) using Google Maps Geocoding API.
        # 4. Normalize schemas of both event types to a unified format.
        # 5. Merge (Flatten) the streams from various sources.
        # 6. Write the unified events to InfluxDB.

        # Example of conceptual unified stream (requires `full_whatsapp_events_pcoll` from another Pub/Sub topic):
        # full_whatsapp_events_pcoll = (
        #     pipeline
        #     | 'ReadFullWhatsAppEvents' >> beam.io.ReadFromPubSub(subscription=f"projects/{config.GCP_PROJECT_ID}/subscriptions/whatsapp-full-events-data-sub")
        #     | 'ProcessWhatsAppEventWithAI' >> beam.ParDo(transforms.AIComprehensionFn()) # Apply AI
        # )

        # processed_tweets_pcoll = (
        #     raw_twitter_feed_pcoll
        #     | 'ProcessTweetWithAI' >> beam.ParDo(transforms.AIComprehensionFn()) # Apply AI to tweets
        #     | 'GeocodeTweetLocations' >> beam.ParDo(transforms.GeocodeLocationFn()) # Geocode if needed
        # )

        # unified_data_stream = (
        #     (full_whatsapp_events_pcoll, processed_tweets_pcoll)
        #     | 'FlattenAllProcessedEvents' >> beam.Flatten()
        #     | 'WriteToUnifiedSink' >> io_connectors.WriteToInfluxDB()
        # )


if __name__ == '__main__':
    # --- IMPORTANT: Set these environment variables in your terminal BEFORE running this script ---
    # Example:
    # export GCP_PROJECT_ID='schrodingers-cat-466413'
    # export DATAFLOW_REGION='asia-south1' # Must be a Dataflow-supported region
    # export PUBSUB_SUBSCRIPTION_NAME_TRIGGER='vectraCityAI-event-trigger-sub'
    # export PUBSUB_SUBSCRIPTION_NAME_TWITTER='raw-twitter-feed-sub' # Create this topic/subscription!
    # export GCS_DATAFLOW_BUCKET='schrodingers-cat-466413-dataflow-temp' # Ensure this GCS bucket exists!

    print("Starting Apache Beam pipeline")

    # --- How to Run ---
    # 1. Local (for testing, uses DirectRunner):
    #    python -m src.main_pipeline --runner=DirectRunner --streaming
    #
    # 2. Deploy to Google Cloud Dataflow (for production):
    #    python -m src.main_pipeline \
    #      --runner=DataflowRunner \
    #      --project=$GCP_PROJECT_ID \
    #      --region=$DATAFLOW_REGION \
    #      --temp_location=$GCS_TEMP_LOCATION \
    #      --staging_location=$GCS_STAGING_LOCATION \
    #      --streaming \
    #      --job_name=unified-city-pulse-pipeline-$(date +%Y%m%d%H%M%S) \
    #      --max_num_workers=2 # Adjust worker count as needed

    run_pipeline()
    print("Apache Beam pipeline finished its setup. It will continue running for streaming data.")