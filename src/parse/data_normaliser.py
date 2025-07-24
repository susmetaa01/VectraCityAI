# src/transforms/data_normalizer.py
import apache_beam as beam
import json
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger('beam_data_normalizer')
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def _normalize_event_fields(event_data: Dict[str, Any], source_type: str) -> Dict[str, Any]:
    """
    Normalizes event data from different sources into a common, consistent format.
    This is a template; you'll expand this with specific logic for each source type.
    """
    logger.info(f"--- Normalizing Event from {source_type} ---")
    logger.info(f"Raw incoming event data for normalization: {json.dumps(event_data, indent=2)}")
    logger.info("---------------------------------------------")

    normalized_event = {
        "event_id": event_data.get('message_sid') or event_data.get('tweet_id') or event_data.get('article_id') or str(uuid.uuid4()), # Unique ID
        "source": source_type,
        "raw_data_original": event_data, # Keep original raw data for auditing/debugging

        "text_content": None,
        "timestamp_utc": None,
        "latitude": None,
        "longitude": None,
        "address_info": None, # Could be string or dict
        "media_gcs_uri": None,
        "media_content_type": None,
        "keywords": [], # Normalized keywords/hashtags
        "sentiment": None, # Pre-fill, will be populated by AI later
        "error_flags": [] # To flag issues during processing
    }

    # --- Source-Specific Normalization Logic ---

    if source_type == "whatsapp":
        normalized_event["text_content"] = event_data.get('text_body')
        normalized_event["timestamp_utc"] = event_data.get('timestamp')
        normalized_event["latitude"] = float(event_data['latitude']) if event_data.get('latitude') is not None else None
        normalized_event["longitude"] = float(event_data['longitude']) if event_data.get('longitude') is not None else None
        normalized_event["address_info"] = event_data.get('address') or event_data.get('label')
        normalized_event["media_gcs_uri"] = event_data.get('media_gcs_uri')
        normalized_event["media_content_type"] = event_data.get('media_content_type')
        normalized_event["keywords"] = [k.lower() for k in normalized_event["text_content"].split() if len(k) > 2] # Simple text split for keywords

    elif source_type == "twitter":
        normalized_event["text_content"] = event_data.get('text')
        normalized_event["timestamp_utc"] = event_data.get('created_at') # Needs parsing from Twitter's format
        normalized_event["latitude"] = float(event_data['geo_coordinates_lat']) if event_data.get('geo_coordinates_lat') is not None else None
        normalized_event["longitude"] = float(event_data['geo_coordinates_lon']) if event_data.get('geo_coordinates_lon') is not None else None
        normalized_event["address_info"] = event_data.get('place_name')
        normalized_event["keywords"] = [h.lower() for h in event_data.get('hashtags', [])]
        # Twitter media would typically be part of text or attached URLs that need separate fetching/processing if desired beyond GCS

    elif source_type == "googlenews":
        normalized_event["text_content"] = event_data.get('summary') or event_data.get('title')
        normalized_event["timestamp_utc"] = event_data.get('published_date') # Needs parsing
        normalized_event["address_info"] = None # Will need Geocoding based on text content later
        normalized_event["keywords"] = [k.lower() for k in normalized_event["text_content"].split() if len(k) > 2] # Simple text split for keywords

    # --- Common Post-Normalization Steps ---
    # Standardize timestamp to ISO format if possible
    if normalized_event["timestamp_utc"]:
        try:
            import dateutil.parser # This library is very robust for parsing various date strings
            normalized_event["timestamp_utc"] = dateutil.parser.parse(normalized_event["timestamp_utc"]).isoformat()
        except Exception:
            normalized_event["error_flags"].append("timestamp_parse_error")
            normalized_event["timestamp_utc"] = datetime.utcnow().isoformat() # Fallback to current UTC time

    return normalized_event


class ComprehendFn(beam.DoFn):
    """
    A Beam DoFn that applies the comprehend_event logic to incoming data,
    normalizing it into a common schema.
    """
    def process(self, element: Dict[str, Any]):
        # Element should already be a parsed dictionary from its source.
        # Determine source type based on unique identifiers present in the element
        source_type = "unknown"
        if "message_sid" in element and "from_number" in element:
            source_type = "whatsapp"
        elif "tweet_id" in element and "user_id" in element:
            source_type = "twitter"
        elif "article_id" in element and "source" in element and element["source"] == "Google News RSS":
            source_type = "googlenews"

        yield _normalize_event_fields(element, source_type)