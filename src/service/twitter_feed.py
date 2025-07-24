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