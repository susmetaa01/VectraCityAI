import os
import tweepy # pip install tweepy
import json
from google.cloud import pubsub_v1 # pip install google-cloud-pubsub

# --- Configuration ---
# Twitter API Credentials (Set these as environment variables)
BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN") # For Twitter API v2 StreamingClient

# Google Cloud Pub/Sub Configuration (Set these as environment variables)
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "schrodingers-cat-466413")
PUBSUB_TOPIC_ID_RAW_TWITTER = os.getenv("PUBSUB_TOPIC_ID_RAW_TWITTER", "raw-twitter-feed")

# --- Pub/Sub Publisher Client ---
publisher = pubsub_v1.PublisherClient()
PUBSUB_TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID_RAW_TWITTER)

# --- Twitter Streaming Client Listener ---
class TwitterStreamListener(tweepy.StreamingClient):
    """
    Custom Tweepy StreamingClient to handle incoming tweets and publish to Pub/Sub.
    """
    def on_tweet(self, tweet):
        """Called when a new tweet is received."""
        print(f"Received tweet (ID: {tweet.id}): {tweet.text[:50]}...")

        # Convert tweet object to JSON string
        # tweet.data contains the raw JSON dict from Twitter API v2
        tweet_json = json.dumps(tweet.data)

        try:
            # Publish to Pub/Sub
            future = publisher.publish(PUBSUB_TOPIC_PATH, tweet_json.encode('utf-8'))
            print(f"Published tweet to Pub/Sub with ID: {future.result()}")
        except Exception as e:
            print(f"Error publishing tweet to Pub/Sub: {e}")

    def on_includes(self, includes):
        """Called when includes field is present in the streamed data."""
        # For example, when you request expansions like author or media.
        # print(f"Includes: {includes}")
        pass

    def on_errors(self, errors):
        """Called when an error occurs during streaming."""
        print(f"Streaming Error: {errors}")

    def on_connection_error(self):
        """Called when connection error occurs (e.g., timeout)."""
        print("Connection error. Stream will retry.")
        self.disconnect() # Disconnect and let the main loop retry connection.

    def on_exception(self, exception):
        """Called when an internal exception occurs in Tweepy."""
        print(f"Tweepy Exception: {exception}")

    def on_request_error(self, status_code):
        """Called when a non-200 HTTP status code is returned by the API."""
        print(f"HTTP Request Error: {status_code}")
        # Return False to stop the stream
        if status_code == 401:
            print("Authentication error. Check your Bearer Token.")
            return False # Stop the stream on auth errors
        if status_code == 429:
            print("Rate limit exceeded. Waiting for retry...")
            # Tweepy's StreamingClient has built-in retry logic, so just return True
            return True
        return True # Continue streaming by default for other errors

    def on_disconnect(self):
        """Called when the stream is disconnected."""
        print("Stream disconnected.")


# --- Main Execution Logic ---
if __name__ == "__main__":
    if not BEARER_TOKEN:
        print("Error: TWITTER_BEARER_TOKEN environment variable not set.")
        print("Please set it before running the script.")
        exit(1)

    print("Initializing Twitter Stream API client...")
    listener = TwitterStreamListener(BEARER_TOKEN)

    # --- Manage Filtered Stream Rules ---
    # These rules tell Twitter which tweets to send you.
    # You can specify keywords, hashtags, user IDs, geographic areas (if eligible).
    # Be mindful of the character limit for rules.
    # Documentation: https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule

    # Example Rules for Bengaluru context:
    # Use 'is:retweet' to exclude retweets if you only want original content
    # Use 'lang:en' to filter by language
    # You can also add geo:bounding_box rules if your access level supports it

    rules = [
        # General Bengaluru civic issues / traffic
        tweepy.StreamRule("Bengaluru OR Bangalore (traffic OR accident OR flood OR waterlogging OR protest OR powercut OR road OR metro OR fire) -is:retweet lang:en"),
        # Major events / places
        tweepy.StreamRule("(CubbonPark OR MGroad OR BrigadeRoad OR Indiranagar OR Koramangala) (event OR festival OR crowd OR police OR jam OR closure) -is:retweet lang:en"),
        # Common civic hashtags
        tweepy.StreamRule("#BengaluruTraffic OR #BBMP OR #BangaloreRains OR #BLRLocksDown OR #BengaluruFloods -is:retweet lang:en"),
        # Specific service complaints
        tweepy.StreamRule("BESCOM OR BWSSB (complaint OR issue OR no_water OR no_power OR fault OR outage) Bengaluru -is:retweet lang:en")
    ]

    # You might want to delete all existing rules before adding new ones, especially during development
    # This ensures you're only getting data for your current rules.
    existing_rules = listener.get_rules().data
    if existing_rules:
        rule_ids = [rule.id for rule in existing_rules]
        print(f"Deleting existing rules: {rule_ids}")
        listener.delete_rules(rule_ids)
        print("Existing rules deleted.")
    else:
        print("No existing rules found.")

    print(f"Adding {len(rules)} new rules to the stream...")
    add_rules_response = listener.add_rules(rules)
    if add_rules_response.errors:
        print(f"Errors adding rules: {add_rules_response.errors}")
        exit(1)
    else:
        print(f"Rules added successfully: {add_rules_response.data}")

    # --- Start the Stream ---
    print("\nStarting Twitter stream... Waiting for tweets matching rules.")
    print("This script will run continuously. Press Ctrl+C to stop.")

    try:
        # Start streaming. This call blocks indefinitely.
        # You can add expansions here to get more data if needed, e.g., tweet_fields=["geo", "public_metrics"]
        listener.filter(tweet_fields=["geo", "public_metrics"]) # request geo and public_metrics fields
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Stopping stream.")
    finally:
        listener.disconnect()
        print("Twitter stream publisher stopped.")