# src/transforms/raw_data_parser.py
import apache_beam as beam
import json
import logging

logger = logging.getLogger('beam_raw_data_parser')
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class ParseTweetFn(beam.DoFn):
    """
    Parses raw Twitter JSON payloads from Pub/Sub bytes and extracts relevant fields.
    Assumes standard Twitter API v1.1 or v2 tweet structure.
    Handles non-tweet control messages (like delete notifications) by skipping them.
    """
    def process(self, element):
        try:
            tweet_json = json.loads(element.decode('utf-8'))

            # Skip non-tweet messages (e.g., delete notifications, which don't have text)
            if 'text' not in tweet_json and 'full_text' not in tweet_json:
                if 'delete' in tweet_json:
                    logger.info(f"Skipping tweet delete notification: {tweet_json.get('delete', {}).get('status', {}).get('id_str')}")
                else:
                    logger.info(f"Skipping non-text tweet or unknown control message: {tweet_json.keys()}")
                return # Do not yield anything for these elements

            parsed_tweet = {
                'tweet_id': tweet_json.get('id_str', tweet_json.get('id')),
                'text': tweet_json.get('full_text', tweet_json.get('text')),
                'created_at': tweet_json.get('created_at'),
                'user_id': tweet_json.get('user', {}).get('id_str', tweet_json.get('user', {}).get('id')),
                'user_screen_name': tweet_json.get('user', {}).get('screen_name'),
                'source': 'twitter', # Explicitly set source type
                'lang': tweet_json.get('lang'),
                'is_quote_status': tweet_json.get('is_quote_status', False),
                'retweet_count': tweet_json.get('retweet_count', 0),
                'favorite_count': tweet_json.get('favorite_count', 0),
                'geo_coordinates_lon': None,
                'geo_coordinates_lat': None,
                'place_name': None,
                'place_type': None,
                'bbox_coordinates': None,
                'hashtags': [h['text'] for h in tweet_json.get('entities', {}).get('hashtags', [])],
                'user_mentions': [{'id': m['id_str'], 'screen_name': m['screen_name']}
                                  for m in tweet_json.get('entities', {}).get('user_mentions', [])],
            }

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
            logger.error(f"Error decoding JSON from Pub/Sub (Twitter feed): {e} - Element: {element.decode('utf-8', errors='ignore')}")
            # Consider yielding an error object to a dead-letter queue
        except Exception as e:
            logger.error(f"Error parsing tweet: {e} - Element: {element.decode('utf-8', errors='ignore')}")
            # Consider yielding an error object to a dead-letter queue