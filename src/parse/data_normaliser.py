# src/transforms/data_normalizer.py
import apache_beam as beam
import json
import logging
import uuid # <-- NEW: For generating UUIDs if needed for event_id
from datetime import datetime
from typing import Dict, Any, Optional

# Relative imports from your project structure
from ..model.incoming_events import AnalyzeInput, Geolocation, DataInput # <-- UPDATED: Import AnalyzeInput, DataInput
from ..utils.geocoding_utils import reverse_geocode, GeocodingError # <-- NEW: For reverse geocoding

logger = logging.getLogger('beam_data_normalizer')
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def _normalize_event_fields(event_data: Dict[str, Any], source_type: str) -> Dict[str, Any]:
    """
    Normalizes event data from different sources into a common format
    that directly maps to the AnalyzeInput schema for AI comprehension.
    """
    logger.info(f"--- Starting Normalization from {source_type} ---")
    logger.info(f"Raw incoming event data for normalization: {json.dumps(event_data, indent=2)}")

    # Initialize fields for AnalyzeInput
    information_text: Optional[str] = None
    media_gcs_uri: Optional[str] = None
    media_content_type: Optional[str] = None
    event_timestamp_str: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address_info: Optional[str] = None
    optional_info_parts: List[str] = [] # Collect various extra details here

    # --- Source-Specific Extraction Logic ---

    if source_type == "whatsapp":
        information_text = event_data.get('text_body')
        event_timestamp_str = event_data.get('timestamp')
        latitude = float(event_data['latitude']) if event_data.get('latitude') is not None else None
        longitude = float(event_data['longitude']) if event_data.get('longitude') is not None else None
        address_info = event_data.get('address') or event_data.get('label')
        media_gcs_uri = event_data.get('media_gcs_uri')
        media_content_type = event_data.get('media_content_type')
        if event_data.get('location_resolved'): optional_info_parts.append("Location was user-provided.")
        # Add other WhatsApp-specific fields to optional_info_parts if relevant

    elif source_type == "twitter":
        information_text = event_data.get('text')
        event_timestamp_str = event_data.get('created_at') # Needs robust parsing
        latitude = float(event_data['geo_coordinates_lat']) if event_data.get('geo_coordinates_lat') is not None else None
        longitude = float(event_data['geo_coordinates_lon']) if event_data.get('geo_coordinates_lon') is not None else None
        address_info = event_data.get('place_name')
        optional_info_parts.append(f"Tweet ID: {event_data.get('tweet_id')}, User: @{event_data.get('user_screen_name')}")
        if event_data.get('hashtags'): optional_info_parts.append(f"Hashtags: {', '.join(event_data['hashtags'])}")
        # Twitter media typically handled separately or linked via URLs in text, not direct GCS upload from bot

    elif source_type == "googlenews":
        information_text = event_data.get('summary') or event_data.get('title')
        event_timestamp_str = event_data.get('published_date') # Needs robust parsing
        address_info = event_data.get('address_info') # Might already be populated if geocoded by ingestor
        optional_info_parts.append(f"Source: Google News, Article ID: {event_data.get('article_id')}, Link: {event_data.get('link')}")
        # News articles typically don't have media for direct AI comprehension unless explicitly extracted

    # --- Standardize Timestamp ---
    if event_timestamp_str:
        try:
            import dateutil.parser # This library is very robust for parsing various date strings
            event_timestamp_str = dateutil.parser.parse(event_timestamp_str).isoformat()
        except Exception:
            logger.warning(f"Could not parse timestamp '{event_timestamp_str}' for event. Falling back to now.")
            event_timestamp_str = datetime.utcnow().isoformat() # Fallback

    # --- Reverse Geocoding (if lat/lon are present but no full address) ---
    # This ensures comprehensive location info for AI and maps, if not already provided
    if latitude is not None and longitude is not None:
        try:
            geo_details = reverse_geocode(latitude, longitude)
            if geo_details:
                # Prioritize existing address if explicit, otherwise use geocoded
                address_info = address_info or geo_details.get("formatted_address")
                # Populate area/sublocation from geocoding
                geo_area = geo_details.get("area")
                geo_sublocation = geo_details.get("sublocation")
            else:
                geo_area, geo_sublocation = None, None
        except GeocodingError as e:
            logger.error(f"Reverse geocoding failed for ({latitude}, {longitude}): {e}")
            geo_area, geo_sublocation = None, None
            optional_info_parts.append(f"Geocoding Error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during reverse geocoding for ({latitude}, {longitude}): {e}")
            geo_area, geo_sublocation = None, None
            optional_info_parts.append(f"Unexpected Geocoding Error: {e}")
    else:
        geo_area, geo_sublocation = None, None # No lat/lon, no geocoding

    # --- Construct Geolocation Pydantic Model ---
    geolocation_obj = Geolocation(
        latitude=latitude,
        longitude=longitude,
        address=address_info,
        area=geo_area,
        sublocation=geo_sublocation
    )

    # --- Construct DataInput Pydantic Model (if media exists) ---
    data_input_obj: Optional[DataInput] = None
    if media_gcs_uri and media_content_type:
        try:
            data_input_obj = DataInput(url=media_gcs_uri, mimeType=media_content_type)
        except Exception as e:
            logger.error(f"Error creating DataInput for media URI {media_gcs_uri}: {e}")
            optional_info_parts.append(f"Media Input Error: {e}")


    # --- Construct the final AnalyzeInput compatible dictionary ---
    # This dictionary will be passed to AIComprehensionFn
    analyze_input_payload = {
        "data": data_input_obj.model_dump() if data_input_obj else None, # Use model_dump() for dict representation
        "information": information_text,
        "geolocation": geolocation_obj.model_dump(), # Use model_dump() for dict representation
        "timestamp": event_timestamp_str,
        "optional_information": "\n".join(optional_info_parts) if optional_info_parts else None
    }

    logger.info(f"--- Finished Normalization for {source_type} ---")
    logger.info(f"Normalized AnalyzeInput Payload: {json.dumps(analyze_input_payload, indent=2)}")
    logger.info("---------------------------------------------")

    return analyze_input_payload


class ComprehendFn(beam.DoFn):
    """
    A Beam DoFn that applies the _normalize_event_fields logic to incoming data,
    normalizing it into a common schema (AnalyzeInput compatible dictionary).
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