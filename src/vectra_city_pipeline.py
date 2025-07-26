import json
import logging  # Keep logging import if you use it directly here
from datetime import datetime  # Keep datetime if used elsewhere

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, \
    GoogleCloudOptions

from src.parse.parse_gnews import ExtractNewsMetadataTransform
# Import necessary BigQuery components for general BigQueryIO usage if needed elsewhere,
# but for SQL inserts, BigQueryDisposition is sufficient for create_disposition.
# from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteDisposition, BigQueryIO

# Import the new BigQuerySqlInsertFn from your custom module
from .publish.big_query import BigQuerySqlInsertFn  # <--- NEW IMPORT

import os
from dotenv import load_dotenv

# Relative imports from your newly created 'src' package
from . import config
from . import io_connectors
from . import transform
from . import parse
from . import analyze

from .parse import raw_data_parser  # For ParseTweetFn
from .parse import data_normaliser  # For ComprehendFn
from .analyze import gemini_analyzer

# Set up logging for the Beam pipeline (if not already done globally)
logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger(__name__)


def run_pipeline():
    """
    Main function to define and run the Apache Beam pipeline.
    This pipeline reads from WhatsApp trigger events and raw Twitter feeds,
    and writes analyzed WhatsApp events to BigQuery.
    """
    pipeline_options = PipelineOptions()
    load_dotenv()

    # Configure common pipeline options for Dataflow deployment
    pipeline_options.view_as(GoogleCloudOptions).project = config.GCP_PROJECT_ID
    pipeline_options.view_as(GoogleCloudOptions).region = config.DATAFLOW_REGION
    pipeline_options.view_as(GoogleCloudOptions).temp_location = config.GCS_TEMP_LOCATION
    pipeline_options.view_as(GoogleCloudOptions).staging_location = config.GCS_STAGING_LOCATION
    pipeline_options.view_as(StandardOptions).streaming = True  # Essential for Pub/Sub sources

    # Define the BigQuery table name parts
    bigquery_dataset = config.BIGQUERY_DATASET_ID
    bigquery_table = config.BIGQUERY_TABLE_ID
    bigquery_project = config.GCP_PROJECT_ID

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # --- Branch 1: Full WhatsApp Event Payloads ---
        whatsapp_pipeline = (
                pipeline
                | 'ReadFullWhatsAppEvents' >> beam.io.ReadFromPubSub(
            subscription=f"projects/{config.GCP_PROJECT_ID}/subscriptions/{config.PUBSUB_TOPIC_ID_INCOMING}-sub"
        )
                | 'DecodeAndParseFullWhatsAppJson' >> beam.Map(
            lambda element: json.loads(element.decode('utf-8')))
                | 'ComprehendWhatsAppEvent' >> beam.ParDo(parse.data_normaliser.ComprehendFn())
                | 'AnalyzeWhatsAppEventsWithGemini' >> beam.ParDo(
            analyze.gemini_analyzer.AIComprehensionFn())
                | 'WriteWhatsAppAnalyzedToBigQuerySql' >> beam.ParDo(BigQuerySqlInsertFn()
                                                                     )
        )

        google_news_pipeline = (
                pipeline
                | 'ReadRawGoogleNewsFeed' >> io_connectors.ReadGoogleNewsFeed()
                | 'DecodeAndParseNewsJson' >> beam.Map(
            lambda element: json.loads(element.decode('utf-8')))
                | 'WriteGnewsAnalyzedToBigQuerySql' >> beam.ParDo(BigQuerySqlInsertFn()
                                                                     )
        )

        # --- Branch 2: Raw Twitter Feed ---
        # Raw data -> Parsing -> Normalization -> AI Analysis
        twitter_pipeline = (
                pipeline
                | 'ReadRawTwitterFeed' >> io_connectors.ReadTwitterFeed()
                | 'ParseTwitterTweetData' >> beam.ParDo(raw_data_parser.ParseTweetFn())
                # | 'PrintParsedTweet' >> beam.Map(lambda x: print(f"Parsed Twitter Tweet: {x}")) # Debug parsed
                | 'ComprehendTwitterEvent' >> beam.ParDo(data_normaliser.ComprehendFn())
                #     | 'PrintTwitterComprehendedEvent' >> beam.Map(
                # lambda x: print(f"Comprehended Twitter Event: {x}"))  # Debug normalized
                | 'AnalyzeTwitterEventsWithGemini' >> beam.ParDo(
            gemini_analyzer.AIComprehensionFn())
                | 'PrintTwitterAnalyzedEvent' >> beam.Map(
            lambda x: print(f"Analyzed Twitter Event: {x}"))
        )

        # --- Branch 3: Raw Google News Feed ---
        # Raw data -> Decoding/Parsing -> Normalization -> AI Analysis


        # --- Trigger Event Listener (Separate Branch, No AI Analysis on triggers) ---
        # This branch reads only the SIDs of WhatsApp events that have been fully processed.
        # whatsapp_trigger_listener = (
        #         pipeline
        #         | 'ReadWhatsAppTriggerEvents' >> io_connectors.ReadTriggerEvents()
        #         | 'PrintWhatsAppTriggerPayload' >> beam.Map(
        #     lambda x: print(f"WhatsApp Trigger Event (SID only): {x}"))
        # )

        # --- Final Sink ---
        # The analyzed streams (whatsapp_analyzed_events, twitter_analyzed_events, news_analyzed_articles)
        # would then be merged and written to a final sink like InfluxDB.

        # Example of conceptual unified stream:
        # unified_analyzed_stream = (
        #     (whatsapp_analyzed_events, twitter_analyzed_events, news_analyzed_articles)
        #     | 'FlattenAllAnalyzedEvents' >> beam.Flatten()
        #     # | 'WriteToUnifiedSink' >> io_connectors.WriteToInfluxDB() # Your InfluxDB PTransform
        # )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO)  # Ensure logging is configured for main execution as well
    _LOGGER.info("Starting Apache Beam pipeline")

    # --- IMPORTANT: Set these environment variables in your terminal BEFORE running this script ---
    # Example:
    # export GCP_PROJECT_ID='schrodingers-cat-466413'
    # export DATAFLOW_REGION='asia-south1' # Must be a Dataflow-supported region
    # export PUBSUB_SUBSCRIPTION_NAME_TRIGGER='vectraCityAI-event-trigger-sub'
    # export PUBSUB_SUBSCRIPTION_NAME_TWITTER='raw-twitter-feed-sub' # Create this topic/subscription!
    # export GCS_DATAFLOW_BUCKET='schrodingers-cat-466413-dataflow-temp' # Ensure this GCS bucket exists!
    # export BIGQUERY_DATASET_ID='your_bigquery_dataset' # Already set in config.py to 'vectraCityRaw'
    # export BIGQUERY_TABLE_ID='whatsapp_analyzed_events' # Already set in config.py

    # --- How to Run ---
    # 1. Local (for testing, uses DirectRunner):
    #    python -m src.vectra_city_pipeline --runner=DirectRunner --streaming
    #
    # 2. Deploy to Google Cloud Dataflow (for production):
    #    python -m src.vectra_city_pipeline \
    #      --runner=DataflowRunner \
    #      --project=$GCP_PROJECT_ID \
    #      --region=$DATAFLOW_REGION \
    #      --temp_location=$GCS_TEMP_LOCATION \
    #      --staging_location=$GCS_STAGING_LOCATION \
    #      --streaming \
    #      --job_name=unified-city-pulse-pipeline-$(date +%Y%m%d%H%M%S) \
    #      --max_num_workers=2 # Adjust worker count as needed

    run_pipeline()
    _LOGGER.info(
        "Apache Beam pipeline finished its setup. It will continue running for streaming data.")
