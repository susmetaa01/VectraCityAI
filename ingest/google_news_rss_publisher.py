import json
import os
import time  # For sleeping between polls
from datetime import datetime, timedelta
from typing import List

from dotenv import load_dotenv
from google import genai
from google.cloud import pubsub_v1  # For publishing to Pub/Sub
from google.genai import types

from ingest.gnews_prompts import get_google_news_prompt, get_structured_news_prompt
from src.model.incoming_events import AnalysisResponse

# --- Configuration ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'schrodingers-cat-466413')
PUBSUB_TOPIC_GNEWS_RAW_NEWS = os.getenv('PUBSUB_TOPIC_GNEWS_RAW_NEWS', 'gnews-incoming-raw-events') # NEW Pub/Sub topic for news


POLLING_INTERVAL_SECONDS = 120 # Poll every 5 minutes (300 seconds)

# --- Pub/Sub Publisher Client ---
publisher = pubsub_v1.PublisherClient()
PUBSUB_TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_GNEWS_RAW_NEWS)

# --- In-memory cache for de-duplication ---
# Stores (feed_url, article_id/link) to avoid reprocessing. Clears periodically.
processed_article_cache = set()
CACHE_CLEANUP_INTERVAL_SECONDS = 3600 # Clear cache every hour

current_time = datetime.now()

area = ["bangalore"]
news_tags = ["traffic, floods, power cut, bbmp, municipality, drainage, protests, accidents, rallies, events"]
news_timedelta_days = 10
client = genai.Client()

load_dotenv()


def parse_google_news():
    grounding_tool = types.Tool(
        google_search=types.GoogleSearch()
    )

    # Configure generation settings
    config = types.GenerateContentConfig(
        tools=[grounding_tool]
    )

    start_time = current_time - timedelta(days=news_timedelta_days)

    # Make the request
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=get_google_news_prompt(start_time, current_time, area, news_tags),
        config=config,
    )

    news_content = response.candidates[0].content.parts[0]

    if news_content == "NO_NEWS":
        return None

    structured_news = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=get_structured_news_prompt(news_content),
        config={
            "response_mime_type": "application/json",
            "response_schema": list[AnalysisResponse],
        },
    )

    return structured_news.parsed


# --- Function to fetch and publish a single RSS feed ---
def fetch_and_publish_feed():
    print(f"Search news for areas = {area} and date = [{datetime.now()}] and delta={news_timedelta_days}")
    try:
        responses: List[AnalysisResponse] = parse_google_news()
        if responses is None:
            pass
        for res in responses:
            # Publish to Pub/Sub
            json_payload = json.dumps(res, ensure_ascii=False)  # ensure_ascii=False for proper unicode
            future = publisher.publish(PUBSUB_TOPIC_PATH, json_payload.encode('utf-8'))
            print(f"Published {res} new articles.")
    except Exception as e:
        print(f"Error fetching or processing feed {current_time}: {e}")

# --- Main Polling Loop ---
if __name__ == "__main__":
    fetch_and_publish_feed()

    print(f"[{datetime.now()}] All feeds checked. Sleeping for {POLLING_INTERVAL_SECONDS} seconds...")
    time.sleep(POLLING_INTERVAL_SECONDS)
