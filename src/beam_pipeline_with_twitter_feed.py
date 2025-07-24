import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
import os
import json

# --- Configuration for Google Cloud Project and Pub/Sub ---
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'schrodingers-cat-466413') # Replace with your actual project ID

# Existing Pub/Sub for WhatsApp trigger events
PUBSUB_SUBSCRIPTION_NAME_TRIGGER = os.environ.get('PUBSUB_SUBSCRIPTION_NAME_TRIGGER', 'vectraCityAI-event-trigger-sub')
PUBSUB_SUBSCRIPTION_PATH_TRIGGER = f"projects/{PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_NAME_TRIGGER}"

# NEW: Pub/Sub for raw Twitter feed data
# Ensure this topic and subscription are created and actively receiving tweets
PUBSUB_SUBSCRIPTION_NAME_TWITTER = os.environ.get('PUBSUB_SUBSCRIPTION_NAME_TWITTER', 'twitter-incoming-raw-events-sub')
PUBSUB_SUBSCRIPTION_PATH_TWITTER = f"projects/{PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_NAME_TWITTER}"


# --- DoFn for parsing raw Twitter JSON ---
class ParseTweetFn(beam.DoFn):
    """
    Parses raw Twitter JSON payloads and extracts relevant fields.
    Assumes standard Twitter API v1.1 or v2 tweet structure.
    """
    def process(self, element):
        try:
            # Pub/Sub messages are bytes, decode and parse the JSON payload
            tweet_json = json.loads(element.decode('utf-8'))

            # Simple check to ensure it's a valid tweet and not a control message (e.g., status delete)
            if 'text' not in tweet_json and 'full_text' not in tweet_json:
                if 'delete' in tweet_json:
                    print(f"Skipping tweet delete notification: {tweet_json.get('delete', {}).get('status', {}).get('id_str')}")
                else:
                    print(f"Skipping non-text tweet or unknown control message: {tweet_json.keys()}")
                return # Skip this element if it's not a standard tweet with text

            # Extract relevant fields from the tweet object
            # Adjust these fields based on the exact Twitter API payload your ingestion script provides (v1.1 vs v2)
            parsed_tweet = {
                'tweet_id': tweet_json.get('id_str', tweet_json.get('id')), # Use id_str for consistency
                'text': tweet_json.get('full_text', tweet_json.get('text')), # 'full_text' for extended tweets
                'created_at': tweet_json.get('created_at'), # Timestamp string
                'user_id': tweet_json.get('user', {}).get('id_str', tweet_json.get('user', {}).get('id')),
                'user_screen_name': tweet_json.get('user', {}).get('screen_name'),
                'source': tweet_json.get('source'), # e.g., <a href="...">Twitter Web App</a>
                'lang': tweet_json.get('lang'), # Language of the tweet
                'is_quote_status': tweet_json.get('is_quote_status', False),
                'retweet_count': tweet_json.get('retweet_count', 0),
                'favorite_count': tweet_json.get('favorite_count', 0),

                # Geolocation info
                'geo_coordinates_lon': None, # GeoJSON Point is [lon, lat]
                'geo_coordinates_lat': None,
                'place_name': None, # e.g., "Bengaluru, India"
                'place_type': None, # e.g., "city"
                'bbox_coordinates': None, # Bounding box of the place
            }

            # Handle coordinates (GeoJSON Point: [longitude, latitude])
            if tweet_json.get('coordinates') and tweet_json['coordinates'].get('type') == 'Point':
                coords = tweet_json['coordinates']['coordinates']
                parsed_tweet['geo_coordinates_lon'] = coords[0]
                parsed_tweet['geo_coordinates_lat'] = coords[1]

            # Handle place object
            if tweet_json.get('place'):
                place_info = tweet_json['place']
                parsed_tweet['place_name'] = place_info.get('full_name')
                parsed_tweet['place_type'] = place_info.get('place_type')
                if place_info.get('bounding_box') and place_info['bounding_box'].get('coordinates'):
                    parsed_tweet['bbox_coordinates'] = place_info['bounding_box']['coordinates']

            # Hashtags, Mentions (from entities)
            if tweet_json.get('entities'):
                parsed_tweet['hashtags'] = [h['text'] for h in tweet_json['entities'].get('hashtags', [])]
                parsed_tweet['user_mentions'] = [{'id': m['id_str'], 'screen_name': m['screen_name']} for m in tweet_json['entities'].get('user_mentions', [])]

            yield parsed_tweet

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from Pub/Sub (Twitter feed): {e} - Element: {element.decode('utf-8', errors='ignore')}")
        except Exception as e:
            print(f"Error parsing tweet: {e} - Element: {element.decode('utf-8', errors='ignore')}")


# --- Main Pipeline Definition ---
def run_pipeline():
    pipeline_options = PipelineOptions()
    # Configure for DataflowRunner for deployment
    # For local testing, ensure you use --runner=DirectRunner and --streaming
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner' # Or 'DirectRunner'
    pipeline_options.view_as(GoogleCloudOptions).project = PROJECT_ID
    pipeline_options.view_as(GoogleCloudOptions).region = 'asia-south1' # Chennai is in asia-south1. Use a Dataflow-supported region.
    pipeline_options.view_as(GoogleCloudOptions).temp_location = f'gs://{PROJECT_ID}-dataflow-temp/tmp' # Ensure GCS bucket exists
    pipeline_options.view_as(GoogleCloudOptions).staging_location = f'gs://{PROJECT_ID}-dataflow-temp/staging' # Ensure GCS bucket exists
    pipeline_options.view_as(StandardOptions).streaming = True # Essential for Pub/Sub sources

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # --- Branch 1: Existing WhatsApp Trigger Events ---
        # This branch reads signals for processed WhatsApp events
        whatsapp_trigger_events = (
                pipeline
                | 'ReadFromWhatsAppTriggerPubSub' >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION_PATH_TRIGGER)
                | 'DecodeAndParseWhatsAppTriggerJson' >> beam.Map(lambda element: json.loads(element.decode('utf-8')))
                | 'PrintWhatsAppTriggerPayload' >> beam.Map(lambda x: print(f"WhatsApp Trigger: {x}")) # Print with label
        )

        # --- Branch 2: NEW Raw Twitter Feed ---
        # This branch reads raw tweets published from your Twitter ingestion script
        twitter_raw_feed_data = (
                pipeline
                | 'ReadFromRawTwitterPubSub' >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION_PATH_TWITTER)
                | 'ParseRawTweetJson' >> beam.ParDo(ParseTweetFn()) # Use DoFn for structured parsing
                | 'PrintParsedTweet' >> beam.Map(lambda x: print(f"Parsed Tweet: {x}")) # Print with label
        )

        # --- Future: Unified Processing ---
        # In a full project, the parsed Twitter data (twitter_raw_feed_data)
        # would then go through further AI comprehension (Gemini NLP for event type,
        # location extraction, sentiment) and then merge with your processed WhatsApp events
        # (which would come from a different Pub/Sub topic carrying full WhatsApp data,
        # not just triggers).
        # This merged stream would then be sent to InfluxDB.
        #
        # Example (conceptual):
        # unified_processed_events = (
        #     (whatsapp_processed_events_stream, twitter_processed_events_stream)
        #     | 'FlattenAllProcessedEvents' >> beam.Flatten()
        #     | 'WriteToUnifiedSink' >> beam.ParDo(WriteToInfluxDBFn())
        # )


# --- Main execution block ---
if __name__ == '__main__':
    # Set environment variables for local testing (using DirectRunner) or for Dataflow deployment
    # export GCP_PROJECT_ID='schrodingers-cat-466413'
    # export PUBSUB_SUBSCRIPTION_NAME_TRIGGER='vectraCityAI-event-trigger-sub'
    # export PUBSUB_SUBSCRIPTION_NAME_TWITTER='raw-twitter-feed-sub' # NEW subscription, create this!

    print("Starting Apache Beam pipeline to listen for WhatsApp triggers and raw Twitter events...")
    # For local running: `python your_script_name.py --runner=DirectRunner --streaming`
    run_pipeline()
    print("Apache Beam pipeline finished its setup. It will continue running for streaming data.")