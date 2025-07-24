import apache_beam as beam
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import json
import logging
from datetime import datetime
from typing import Optional

import httpx
import requests
from dotenv import load_dotenv
from google import genai
from google.genai import types

from ..model.incoming_events import Geolocation, AnalysisResponse, DataInput


# Relative imports
from . import ai_services # Your AI service functions
from .schemas import Geolocation # For constructing Pydantic models
import dateutil.parser # For robust date parsing


load_dotenv()

client = genai.Client()
logger = logging.getLogger('uvicorn.error')

model_name = 'gemini-2.5-flash-lite-preview-06-17'

class AIComprehensionFn(beam.DoFn):
    def process(self, element: Dict[str, Any]):
        # async def analyze_with_gemini(system_prompt: str, data_input: Optional[DataInput], information: Optional[str],
        #                               geolocation: Geolocation, timestamp: Optional[datetime] = None) -> dict:

        # Prepare the payload for Gemini
        print(f"KHIKHIKHIKHI {element}")
        #
        # data = None
        # data_header = None
        # if data_input is not None:
        #     if data_input.mimeType == 'application/pdf':
        #         doc_data = httpx.get(data_input.url).content
        #         data_header = "Reference Document"
        #         data = types.Part.from_bytes(
        #             data=doc_data,
        #             mime_type='application/pdf'
        #         )
        #     if data_input.mimeType == "image/jpeg":
        #         image_bytes = requests.get(data_input.url).content
        #         data = types.Part.from_bytes(
        #             data=image_bytes, mime_type="image/jpeg"
        #         )
        #         data_header = "Reference Image"
        #     if data_input.mimeType == "video/mp4":
        #         data = client.files.upload(file=data_input.url)
        #         data_header = "Reference Video"
        #     if data_input.mimeType == "audio/mp3":
        #         data = client.files.upload(file=data_input.url)
        #         data_header = "Reference Audio"
        #
        #     if data is None or data_header is None and information is None:
        #         raise ValueError("Unsupported data to comprehend")
        #
        # if timestamp is None:
        #     timestamp = datetime.now()
        #
        # full_prompt = None
        # if data is not None:
        #     full_prompt = [
        #         "\n\n--- Details ---\n",
        #         f"Location: {geolocation}\n",
        #         f"Timestamp: {timestamp}\n",
        #         f"Other optional details: {information}\n",
        #         f"\n--- {data_header} ---\n",
        #         data,
        #         "\n\n--- Analysis Request ---\n",
        #         system_prompt,
        #     ]
        # else:
        #     full_prompt = [
        #         "\n\n--- Details ---\n",
        #         f"Location: {geolocation}\n",
        #         f"Timestamp: {timestamp}\n",
        #         f"Other optional details: {information}\n",
        #         "\n\n--- Analysis Request ---\n",
        #         system_prompt,
        #     ]
        #
        # if full_prompt is None:
        #     logger.error("Prompt is empty")
        #     raise RuntimeError("Prompt is empty")
        #
        # response = client.models.generate_content(
        #     model=model_name,
        #     contents=full_prompt,
        #     config={
        #         "response_mime_type": "application/json",
        #         "response_schema": AnalysisResponse,
        #     },
        # )
        # logger.info(f"Raw Gemini response: {response.text}")
        #
        # try:
        #     # Parse the JSON response from Gemini
        #     parsed_response = json.loads(response.text)
        #     return parsed_response
        # except json.JSONDecodeError as e:
        #     logger.error(f"Error parsing JSON response: {e}")
        #     logger.error(f"Raw response: {response.text}")
        #     # Return a basic error response structure
        #     return {
        #         "error": "Failed to parse response",
        #         "raw_response": response.text
        #     }