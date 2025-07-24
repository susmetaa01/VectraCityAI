import apache_beam as beam
import json

class ParseTweetFn(beam.DoFn):
    """
    Parses raw Twitter JSON payloads and extracts relevant fields for analysis.
    Assumes standard Twitter API v1.1 or v2 tweet structure.
    Handles non-tweet control messages (like delete notifications) by skipping them.
    """
    def process(self, element):
        try:
            # Pub/Sub messages are bytes, decode and parse the JSON payload
            tweet_json = json.loads(element.decode('utf-8'))

            # Skip non-tweet messages (e.g., delete notifications, which don't have text)
            if 'text' not in tweet_json and 'full_text' not in tweet_json:
                if 'delete' in tweet_json:
                    print(f"Skipping tweet delete notification: {tweet_json.get('delete', {}).get('status', {}).get('id_str')}")
                else:
                    print(f"Skipping non-text tweet or unknown control message: {tweet_json.keys()}")
                return # Do not yield anything for these elements

            # Extract relevant fields, handling potential missing keys gracefully
            parsed_tweet = {
                'tweet_id': tweet_json.get('id_str', tweet_json.get('id')),
                'text': tweet_json.get('full_text', tweet_json.get('text')), # 'full_text' for extended tweets
                'created_at': tweet_json.get('created_at'), # Timestamp string (e.g., "Mon Apr 26 06:01:03 +0000 2021")
                'user_id': tweet_json.get('user', {}).get('id_str', tweet_json.get('user', {}).get('id')),
                'user_screen_name': tweet_json.get('user', {}).get('screen_name'),
                'source': tweet_json.get('source'), # e.g., <a href="...">Twitter Web App</a>
                'lang': tweet_json.get('lang'), # Language of the tweet (e.g., "en")
                'is_quote_status': tweet_json.get('is_quote_status', False),
                'retweet_count': tweet_json.get('retweet_count', 0),
                'favorite_count': tweet_json.get('favorite_count', 0),

                # --- Geolocation Information (if available in tweet) ---
                'geo_coordinates_lon': None, # Standard GeoJSON Point is [longitude, latitude]
                'geo_coordinates_lat': None,
                'place_name': None,          # Full name of the place (e.g., "Bengaluru, India")
                'place_type': None,          # Type of place (e.g., "city", "poi")
                'bbox_coordinates': None,    # Bounding box of the place (array of arrays)

                # --- Entities (Hashtags, User Mentions) ---
                'hashtags': [h['text'] for h in tweet_json.get('entities', {}).get('hashtags', [])],
                'user_mentions': [{'id': m['id_str'], 'screen_name': m['screen_name']}
                                  for m in tweet_json.get('entities', {}).get('user_mentions', [])],
            }

            # Populate geolocation fields if available in the tweet JSON
            if tweet_json.get('coordinates') and tweet_json['coordinates'].get('type') == 'Point':
                coords = tweet_json['coordinates']['coordinates']
                parsed_tweet['geo_coordinates_lon'] = coords[0]
                parsed_tweet['geo_coordinates_lat'] = coords[1]

            if tweet_json.get('place'):
                place_info = tweet_json['place']
                parsed_tweet['place_name'] = place_info.get('full_name')
                parsed_tweet['place_type'] = place_info.get('place_type')
                if place_info.get('bounding_box') and place_info['bounding_box'].get('coordinates'):
                    parsed_tweet['bbox_coordinates'] = place_info['bounding_box']['coordinates']

            yield parsed_tweet # Yield the neatly parsed dictionary

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from Pub/Sub (Twitter feed): {e} - Element: {element.decode('utf-8', errors='ignore')}")
            # In a production pipeline, you might yield an error object to a dead-letter queue
        except Exception as e:
            print(f"Error parsing tweet: {e} - Element: {element.decode('utf-8', errors='ignore')}")
            # In a production pipeline, you might yield an error object to a dead-letter queue

# Future: Add other custom PTransforms or DoFns here for AI comprehension, geolocation, schema normalization, etc.
# class AIComprehensionFn(beam.DoFn):
#    def setup(self):
#        # Initialize Google Gemini and Vertex AI clients here
#        pass
#    def process(self, element):
#        # Apply Gemini NLP to text, Vertex AI Vision/Video to media
#        yield processed_element

# class GeocodeLocationFn(beam.DoFn):
#    def setup(self):
#        # Initialize Google Maps Geocoding API client
#        pass
#    def process(self, element):
#        # If 'place_name' exists but no 'geo_coordinates', call Geocoding API
#        yield element_with_geo_coords