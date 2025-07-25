# # src/transforms/ai_analyzer.py
# import apache_beam as beam
# import logging
# from datetime import datetime
# from typing import Dict, Any, Optional
#
# # Relative imports
# from . import ai_services # Your AI service functions
# from ..model.incoming_events import Geolocation # For constructing Pydantic models
# import dateutil.parser # For robust date parsing
#
# logger = logging.getLogger('beam_ai_analyzer')
# if not logger.handlers:
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#
#
# class AIComprehensionFn(beam.DoFn):
#     """
#     A Beam DoFn that calls the Gemini API for AI comprehension of normalized events.
#     This function processes elements from the `ComprehendFn` output.
#     """
#     def setup(self):
#         # Suppress verbose Google API logging from httpx, requests, google.api_core
#         logging.getLogger('google.api_core').setLevel(logging.WARNING)
#         logging.getLogger('httpx').setLevel(logging.WARNING)
#         logging.getLogger('requests').setLevel(logging.WARNING)
#         logger.info("AIComprehensionFn worker setup complete.")
#
#     def process(self, element: Dict[str, Any]):
#         # 'element' here is expected to be a dictionary from data_normalizer.py (normalized_event)
#
#         event_id = element.get('event_id', 'unknown_id')
#         event_source = element.get('source', 'unknown_source')
#         print(f"Processing event {event_id} from {event_source} with AI.")
#
#         # --- Define SYSTEM_PROMPT for Gemini ---
#         SYSTEM_PROMPT = """You are an expert urban analyst for Bengaluru. Based on the provided information,
#         identify the primary type of urban event, summarize it concisely for public consumption,
#         assess its severity, suggest immediate actions or relevant city authorities to be notified,
#         and determine the overall sentiment expressed.
#         Strictly output a JSON object conforming to the AnalysisResponse schema.
#
#         Consider the following event types: Traffic (e.g., jam, accident, closure), Public Transport Issue (bus/metro delay/breakdown),
#         Power Outage, Water Supply Issue, Waste Management (e.g., garbage pile-up, collection issue),
#         Public Health (e.g., disease outbreak, unsanitary conditions), Safety/Crime (e.g., theft, harassment, suspicious activity),
#         Protest/Demonstration, Public Event (e.g., festival, marathon, concert),
#         Disaster/Natural Calamity (e.g., flood, storm, earthquake), Other Civic Issue (e.g., pothole, broken street light, noise complaint),
#         Infrastructure Damage (e.g., fallen tree, damaged building).
#
#         Severity levels: 'Low' (minor inconvenience), 'Medium' (moderate disruption, localized),
#         'High' (significant disruption, widespread), 'Critical' (immediate danger, life-threatening, major infrastructure failure).
#         Sentiment: 'Positive', 'Neutral', 'Negative', 'Mixed'.
#         """
#
#         # --- Map incoming normalized element to analyze_with_gemini arguments ---
#         information_text = element.get('text_content')
#
#         # Geolocation from normalized data
#         latitude = element.get('latitude')
#         longitude = element.get('longitude')
#         address = element.get('address_info')
#         # label field from WhatsApp might be in address_info, or not applicable for Twitter/News
#
#         # Construct Geolocation Pydantic model (ensure types are correct)
#         geolocation_obj = Geolocation(
#             latitude=float(latitude) if latitude is not None else None,
#             longitude=float(longitude) if longitude is not None else None,
#             address=address,
#             label=None # 'label' specific to WhatsApp, can be remapped if needed
#         )
#
#         # Timestamp from normalized data
#         event_timestamp_str = element.get('timestamp_utc')
#         event_timestamp = None
#         if event_timestamp_str:
#             try:
#                 event_timestamp = datetime.fromisoformat(event_timestamp_str) # ISO format from normalizer
#             except ValueError:
#                 logger.warning(f"Could not parse ISO timestamp '{event_timestamp_str}' for event {event_id}. Using current time.")
#                 event_timestamp = datetime.now() # Fallback
#
#         # Media GCS URI and content type from normalized data
#         media_gcs_uri = element.get('media_gcs_uri')
#         media_content_type = element.get('media_content_type')
#
#         # --- Call analyze_with_gemini service function ---
#         gemini_analysis_result = ai_services.analyze_with_gemini(
#             system_prompt=SYSTEM_PROMPT,
#             information=information_text,
#             geolocation=geolocation_obj,
#             timestamp=event_timestamp,
#             media_gcs_uri=media_gcs_uri,
#             media_content_type=media_content_type
#         )
#
#         # Enrich the original element with Gemini's analysis results
#         element['gemini_analysis'] = gemini_analysis_result
#         element['processed_by_gemini'] = True
#         element['processed_at'] = datetime.now().isoformat()
#
#         yield element # Yield the enriched element for the next stage (e.g., InfluxDB)