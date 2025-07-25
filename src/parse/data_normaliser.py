import apache_beam as beam
import json
import logging
import uuid # For generating UUIDs if needed for event_id
from datetime import datetime
from typing import Dict, Any, Optional, List # Added List for type hinting

# Relative imports
# Corrected import path based on likely user refactoring from 'schemas' to 'model.incoming_events'
from ..model.incoming_events import AnalyzeInput, Geolocation, DataInput
from ..utils.geocoding_utils import reverse_geocode, GeocodingError

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

    # Initialize latitude/longitude extracted from source
    extracted_latitude_from_source: Optional[Any] = None
    extracted_longitude_from_source: Optional[Any] = None

    address_info: Optional[str] = None
    optional_info_parts: List[str] = [] # Collect various extra details here

    # --- Source-Specific Extraction Logic ---

    if source_type == "whatsapp":
        information_text = event_data.get('text_body')
        event_timestamp_str = event_data.get('timestamp')
        extracted_latitude_from_source = event_data.get('latitude')
        extracted_longitude_from_source = event_data.get('longitude')
        address_info = event_data.get('address') or event_data.get('label')
        media_gcs_uri = event_data.get('media_gcs_uri')
        media_content_type = event_data.get('media_content_type')
        if event_data.get('location_resolved'): optional_info_parts.append("Location was user-provided.")

    elif source_type == "twitter":
        information_text = event_data.get('text')
        event_timestamp_str = event_data.get('created_at') # Needs robust parsing
        extracted_latitude_from_source = event_data.get('geo_coordinates_lat')
        extracted_longitude_from_source = event_data.get('geo_coordinates_lon')
        address_info = event_data.get('place_name')
        optional_info_parts.append(f"Tweet ID: {event_data.get('tweet_id')}, User: @{event_data.get('user_screen_name')}")
        if event_data.get('hashtags'): optional_info_parts.append(f"Hashtags: {', '.join(event_data['hashtags'])}")

    elif source_type == "googlenews":
        information_text = event_data.get('summary') or event_data.get('title')
        event_timestamp_str = event_data.get('published_date') # Needs robust parsing
        # Google News typically does not have direct lat/lon, so extracted_latitude/longitude will likely be None.
        address_info = event_data.get('address_info') # Might already be populated if geocoded by ingestor
        optional_info_parts.append(f"Source: Google News, Article ID: {event_data.get('article_id')}, Link: {event_data.get('link')}")

    # --- Convert extracted lat/lon to float or ensure None explicitly ---
    latitude_for_geo_obj: Optional[float] = None
    longitude_for_geo_obj: Optional[float] = None

    if extracted_latitude_from_source is not None:
        try:
            latitude_for_geo_obj = float(extracted_latitude_from_source)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert latitude '{extracted_latitude_from_source}' to float. Setting to None.")
            latitude_for_geo_obj = None

    if extracted_longitude_from_source is not None:
        try:
            longitude_for_geo_obj = float(extracted_longitude_from_source)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert longitude '{extracted_longitude_from_source}' to float. Setting to None.")
            longitude_for_geo_obj = None

    # --- Standardize Timestamp ---
    if event_timestamp_str:
        try:
            import dateutil.parser
            event_timestamp_str = dateutil.parser.parse(event_timestamp_str).isoformat()
        except Exception:
            logger.warning(f"Could not parse timestamp '{event_timestamp_str}' for event. Falling back to now.")
            event_timestamp_str = datetime.utcnow().isoformat()

    # --- Reverse Geocoding (if lat/lon are present but no full address) ---
    geo_area, geo_sublocation, geo_formatted_address = None, None, None
    if latitude_for_geo_obj is not None and longitude_for_geo_obj is not None and not address_info:
        try:
            geo_details = reverse_geocode(latitude_for_geo_obj, longitude_for_geo_obj)
            if geo_details:
                geo_formatted_address = geo_details.get("formatted_address")
                geo_area = geo_details.get("area")
                geo_sublocation = geo_details.get("sublocation")
                logger.info(f"Reverse geocoded: Lat/Lon ({latitude_for_geo_obj}, {longitude_for_geo_obj}) -> Area: {geo_area}, Subloc: {geo_sublocation}")
            else:
                logger.warning(f"No geocoding results for ({latitude_for_geo_obj}, {longitude_for_geo_obj}).")
        except GeocodingError as e:
            logger.error(f"Reverse geocoding failed for ({latitude_for_geo_obj}, {longitude_for_geo_obj}): {e}")
            optional_info_parts.append(f"Geocoding Error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during reverse geocoding for ({latitude_for_geo_obj}, {longitude_for_geo_obj}): {e}")
            optional_info_parts.append(f"Unexpected Geocoding Error: {e}")
    else:
        logger.info("Skipping reverse geocoding: Latitude/Longitude missing or address already present.")


    # --- Construct Geolocation Pydantic Model with conditional parameters ---
    geolocation_kwargs: Dict[str, Any] = {}
    if latitude_for_geo_obj is not None:
        geolocation_kwargs['latitude'] = latitude_for_geo_obj
    if longitude_for_geo_obj is not None:
        geolocation_kwargs['longitude'] = longitude_for_geo_obj

    final_address = address_info or geo_formatted_address
    if final_address is not None:
        geolocation_kwargs['address'] = final_address
    if geo_area is not None:
        geolocation_kwargs['area'] = geo_area
    if geo_sublocation is not None:
        geolocation_kwargs['sublocation'] = geo_sublocation

    geolocation_obj = Geolocation(**geolocation_kwargs)


    # --- Construct DataInput Pydantic Model (if media exists) ---
    data_input_obj: Optional[DataInput] = None
    if media_gcs_uri and media_content_type:
        try:
            data_input_obj = DataInput(url=media_gcs_uri, mimeType=media_content_type)
        except Exception as e:
            logger.error(f"Error creating DataInput for media URI {media_gcs_uri}: {e}")
            optional_info_parts.append(f"Media Input Error: {e}")


    # --- Construct the final AnalyzeInput compatible dictionary ---
    analyze_input_payload = {
        "data": data_input_obj.model_dump() if data_input_obj else None,
        "information": information_text,
        "geolocation": geolocation_obj.model_dump(), # Convert Pydantic model to dict
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

        normalized_event = _normalize_event_fields(element, source_type)
        print("NORMALISED EVENT: {}",normalized_event)
        yield normalized_event

        # try: # <-- NEW: Added try-except block for robust processing
        #     normalized_event = _normalize_event_fields(element, source_type)
        #     print("TYPE: {}",type(normalized_event))
        #     yield normalized_event
        # except Exception as e:
        #     # Catch any error during normalization, log it, and discard the element
        #     error_info = {
        #         "error_stage": "ComprehendFn_normalization",
        #         "error_message": str(e),
        #         "original_element_raw": json.dumps(element, indent=2), # Store the original element that caused error
        #         "source_type": source_type,
        #         "timestamp_utc": datetime.now().isoformat()
        #     }
        #     logger.error(f"Error in ComprehendFn for element from {source_type}: {e}\nOriginal Element: {json.dumps(element, indent=2)}", exc_info=True)
        #     # Do NOT yield, effectively dropping the malformed element from the pipeline.
        #     # In a production setting, you might yield error_info to a dead-letter queue.