import json
import logging
from datetime import datetime
from typing import Dict, Any

import apache_beam as beam
import httpx
from google import genai
from google.genai import types

from ..model.incoming_events import AnalysisResponse, DataInput
from ..utils.gcs_utils import fetch_gcs_content_as_bytes

# load_dotenv()

client = genai.Client()
logger = logging.getLogger('uvicorn.error')

model_name = 'gemini-2.5-flash-lite-preview-06-17'

SYSTEM_PROMPT = (
    "Your a urban intelligence analyser. You are responsible for analyzing images, text, video or audio along with geolocation information provided. Given the input do following.\n"
    "1. Categorise the input data provided into one or more of the following categories. Also give relevancy score to it between 0 and 1. Dont categorise anything which is less than 0.5 relevancy.\n"
    "- Road complaints\n- Power or electricity outage\n- Mob immobilisation\n- Heavy traffic congestion\n- Medical and medical requirements\n- Bomb threat\n"
    "2. Generate 2-3 subcategory from the above categories and classify the input.\n"
    "3. Classify to one or more of the following department with relevancy score. Dont classify if score below 0.75\n"
    "- Municipality\n- Police\n- Ambulance\n- Traffic police\n- Fire station\n"
    "Also provide summary of the input provided. Include information from image if given."
)

class AIComprehensionFn(beam.DoFn):
    def process(self, element: Dict[str, Any]):
        # async def analyze_with_gemini(SYSTEM_PROMPT: str, data_input: Optional[DataInput], information: Optional[str],
        #                               geolocation: Geolocation, timestamp: Optional[datetime] = None) -> dict:

        # Prepare the payload for Gemini
        if element is not None:
            print(f"{element}")
        else:
            raise ValueError("Null input for AI comprehension")
        data_input_object = element.get('data')
        data_input = DataInput(**data_input_object) if data_input_object else None
        print(data_input, type(data_input))
        information = element.get('information')
        geolocation = element.get('geolocation')
        timestamp = element.get('timestamp')
        optional_information = element.get('optional_information')
        print(data_input, information, geolocation, timestamp, optional_information)

        data = None
        data_header = None
        if data_input is not None:
            if data_input == 'application/pdf':
                doc_data = httpx.get(data_input.url).content
                data_header = "Reference Document"
                data = types.Part.from_bytes(
                    data=doc_data,
                    mime_type='application/pdf'
                )
            if data_input.mimeType == "image/jpeg":
                # image_bytes = requests.get(data_input.url).content
                image_bytes = fetch_gcs_content_as_bytes(data_input.url)
                data = types.Part.from_bytes(
                    data=image_bytes, mime_type="image/jpeg"
                )
                data_header = "Reference Image"
            if data_input.mimeType == "video/mp4":
                data = client.files.upload(file=data_input.url)
                data_header = "Reference Video"
            if data_input.mimeType == "audio/mp3":
                data = client.files.upload(file=data_input.url)
                data_header = "Reference Audio"

            if data is None or data_header is None and information is None:
                raise ValueError("Unsupported data to comprehend")

        if timestamp is None:
            timestamp = datetime.now()

        full_prompt = None
        if data is not None:
            full_prompt = [
                "\n\n--- Details ---\n",
                f"Location: {geolocation}\n",
                f"Timestamp: {timestamp}\n",
                f"Other optional details: {information}\n",
                f"\n--- {data_header} ---\n",
                data,
                "\n\n--- Analysis Request ---\n",
                SYSTEM_PROMPT,
            ]
        else:
            full_prompt = [
                "\n\n--- Details ---\n",
                f"Location: {geolocation}\n",
                f"Timestamp: {timestamp}\n",
                f"Other optional details: {information}\n",
                "\n\n--- Analysis Request ---\n",
                SYSTEM_PROMPT,
            ]

        if full_prompt is None:
            logger.error("Prompt is empty")
            raise RuntimeError("Prompt is empty")

        response = client.models.generate_content(
            model=model_name,
            contents=full_prompt,
            config={
                "response_mime_type": "application/json",
                "response_schema": AnalysisResponse,
            },
        )
        logger.info(f"Raw Gemini response: {response.text}")

        try:
            # Parse the JSON response from Gemini
            parsed_response = json.loads(response.text)
            return parsed_response
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON response: {e}")
            logger.error(f"Raw response: {response.text}")
            # Return a basic error response structure
            return {
                "error": "Failed to parse response",
                "raw_response": response.text
            }