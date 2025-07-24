import os
import feedparser # For parsing RSS feeds
import json
import time # For sleeping between polls
from datetime import datetime, timedelta
from google.cloud import pubsub_v1 # For publishing to Pub/Sub

# --- Configuration ---
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'schrodingers-cat-466413')
PUBSUB_TOPIC_GNEWS_RAW_NEWS = os.getenv('PUBSUB_TOPIC_GNEWS_RAW_NEWS', 'gnews-incoming-raw-events') # NEW Pub/Sub topic for news

# Google News RSS Feeds for Bengaluru (adjust keywords as needed)
# Use hl=en-IN&gl=IN&ceid=IN:en for India/English specific results
RSS_FEED_URLS = [
    "https://news.google.com/rss/search?q=Bengaluru+traffic&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=Bengaluru+power+cut&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=Bengaluru+BBMP&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=Bengaluru+flood&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=Bengaluru+protest&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=Bengaluru+event&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=Bangalore+accident&hl=en-IN&gl=IN&ceid=IN:en",
    # Add more specific queries for areas/events
    "https://news.google.com/rss/search?q=Koramangala+Bengaluru&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=Indiranagar+Bengaluru&hl=en-IN&gl=IN&ceid=IN:en",
]

POLLING_INTERVAL_SECONDS = 300 # Poll every 5 minutes (300 seconds)

# --- Pub/Sub Publisher Client ---
publisher = pubsub_v1.PublisherClient()
PUBSUB_TOPIC_PATH = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_GNEWS_RAW_NEWS)

# --- In-memory cache for de-duplication ---
# Stores (feed_url, article_id/link) to avoid reprocessing. Clears periodically.
processed_article_cache = set()
CACHE_CLEANUP_INTERVAL_SECONDS = 3600 # Clear cache every hour

last_cache_cleanup_time = datetime.now()

# --- Function to fetch and publish a single RSS feed ---
def fetch_and_publish_feed(feed_url):
    global processed_article_cache, last_cache_cleanup_time

    # Perform periodic cache cleanup
    if datetime.now() - last_cache_cleanup_time > timedelta(seconds=CACHE_CLEANUP_INTERVAL_SECONDS):
        print(f"[{datetime.now()}] Clearing processed article cache (size: {len(processed_article_cache)})")
        processed_article_cache.clear()
        last_cache_cleanup_time = datetime.now()

    print(f"[{datetime.now()}] Fetching feed: {feed_url}")
    try:
        feed = feedparser.parse(feed_url)

        if feed.bozo:
            print(f"  Warning: Bozo error in feed {feed_url}: {feed.bozo_exception}")
            # Consider logging feed.bozo_exception for details

        new_articles_count = 0
        for entry in feed.entries:
            # Google News RSS entries often have a 'link' or 'id' that can serve as a unique identifier
            article_id = entry.get('id') or entry.get('link')

            if article_id and (feed_url, article_id) not in processed_article_cache:
                # Construct a basic payload from the RSS entry
                payload = {
                    'source': 'Google News RSS',
                    'feed_url': feed_url,
                    'article_id': article_id,
                    'title': entry.get('title'),
                    'link': entry.get('link'),
                    'summary': entry.get('summary'),
                    'published_date': entry.get('published'), # Often a string, Beam pipeline can parse it
                    'updated_date': entry.get('updated'),
                    'authors': [author.get('name') for author in entry.get('authors', [])],
                    'raw_entry': {k: v for k, v in entry.items() if k not in ['links', 'guidislink', 'media_content']} # Avoid overly complex objects
                }

                # Publish to Pub/Sub
                try:
                    json_payload = json.dumps(payload, ensure_ascii=False) # ensure_ascii=False for proper unicode
                    future = publisher.publish(PUBSUB_TOPIC_PATH, json_payload.encode('utf-8'))
                    print(f"  Published article '{payload['title'][:50]}...' to Pub/Sub with ID: {future.result()}")
                    processed_article_cache.add((feed_url, article_id)) # Add to cache only on successful publish
                    new_articles_count += 1
                except Exception as e:
                    print(f"  Error publishing article to Pub/Sub: {e} - Article ID: {article_id}")
            # else:
            #     print(f"  Skipping duplicate or unidentifiable article from {feed_url}")

        print(f"  Finished {feed_url}. Published {new_articles_count} new articles.")

    except Exception as e:
        print(f"Error fetching or processing feed {feed_url}: {e}")

# --- Main Polling Loop ---
if __name__ == "__main__":
    print(f"Starting Google News RSS publisher for project {GCP_PROJECT_ID} to topic {PUBSUB_TOPIC_GNEWS_RAW_NEWS}")
    print(f"Polling interval: {POLLING_INTERVAL_SECONDS} seconds.")
    print(f"Number of feeds to monitor: {len(RSS_FEED_URLS)}")

    while True:
        for feed_url in RSS_FEED_URLS:
            fetch_and_publish_feed(feed_url)

        print(f"[{datetime.now()}] All feeds checked. Sleeping for {POLLING_INTERVAL_SECONDS} seconds...")
        time.sleep(POLLING_INTERVAL_SECONDS)