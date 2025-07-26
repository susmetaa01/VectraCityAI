# src/model/incoming_events.py (or src/schemas.py)
import logging
from types import NoneType
from typing import Optional, List, Literal

from pydantic import BaseModel, Field, model_validator  # Ensure Field is imported if used elsewhere

from .geocoding import forward_geocode, reverse_geocode, GeocodingError

# from pydantic import model_validator # No longer needed for Geolocation

logger = logging.getLogger('pydantic_schemas')
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class Geolocation(BaseModel):
    # CRITICAL CHANGE: Simplified definition for true optionality
    latitude: Optional[float] = None # No Field(...) here
    longitude: Optional[float] = None # No Field(...) here

    area: Optional[str] = Field(None, description="Area/city information (auto-populated by normalizer)")
    sublocation: Optional[str] = Field(None, description="Sublocation/neighborhood information (auto-populated by normalizer)")
    address: Optional[str] = Field(None, description="Formatted address (auto-populated by normalizer)")

    @model_validator(mode='after')
    def populate_location_info(self):
        """Automatically populate area, sublocation, and address using reverse geocoding."""
        # Only perform reverse geocoding if the location fields are not already populated
        if self.area is None and self.latitude is NoneType and self.longitude is NoneType:
            logging.error("Area and geo coordinates are None")
            return self
        if self.area is None or self.sublocation is None or self.address is None:
            try:
                logger.info(f'Performing reverse geocoding for latitude={self.latitude}, longitude={self.longitude}')
                geo_dict = reverse_geocode(self.latitude, self.longitude)

                # Only update fields that are None
                if self.area is None:
                    self.area = geo_dict.get("area")
                if self.sublocation is None:
                    self.sublocation = geo_dict.get("sublocation")
                if self.address is None:
                    self.address = geo_dict.get("formatted_address")

                logger.info(f'Reverse geocoding completed: area={self.area}, sublocation={self.sublocation}')

            except GeocodingError as e:
                logger.error(f'Geocoding failed for coordinates ({self.latitude}, {self.longitude}): {e}')
                # Keep the fields as None if geocoding fails

            except Exception as e:
                logger.error(f'Unexpected error during reverse geocoding: {e}')
                # Keep the fields as None if any unexpected error occurs
        if self.latitude is None and self.longitude is None and self.area is not None:
            try:
                logger.info(f'Performing forward geocoding for area={self.area}')
                geo_dict = forward_geocode(self.area)

                # Only update fields that are None
                if self.latitude is None:
                    self.latitude = geo_dict.get("latitude")
                if self.longitude is None:
                    self.longitude = geo_dict.get("longitude")

                logger.info(f'Forward geocoding completed: latitude={self.latitude}, longitude={self.longitude}')
            except GeocodingError as e:
                logger.error(f'Geocoding failed for coordinates ({self.latitude}, {self.longitude}): {e}')
                # Keep the fields as None if geocoding fails

            except Exception as e:
                logger.error(f'Unexpected error during reverse geocoding: {e}')

        return self

    def __str__(self) -> str:
        parts = []
        if self.latitude is not None and self.longitude is not None:
            parts.append(f"Lat: {self.latitude}, Lon: {self.longitude}")
        if self.address:
            parts.append(f"Address: {self.address}")
        elif self.sublocation and self.area:
            parts.append(f"Area: {self.sublocation}, {self.area}")
        elif self.area:
            parts.append(f"Area: {self.area}")

        return ", ".join(parts) if parts else "Location not specified"


class Subcategory(BaseModel):
    category: str
    relevancy_score: float


class ProblemCategory(BaseModel):
    category: str
    relevancy_score: float
    subcategory: List[Subcategory]


class Department(BaseModel):
    department: str
    relevancy_score: float


class AnalysisResponse(BaseModel):
    summary: str
    location: Geolocation
    problem: List[ProblemCategory]
    department: List[Department]


class DataInput(BaseModel):
    url: str = Field(..., description="URL to the data (e.g., GCS URI).")
    mimeType: str = Field(..., description="MIME type of the data (e.g., image/jpeg, video/mp4).")

class AnalyzeInput(BaseModel):
    data: Optional[DataInput] = Field(None, description="Reference to external media data.")
    information: Optional[str] = Field(None, description="Primary text content for analysis.")
    geolocation: Geolocation # Geolocation information, potentially populated by normalization
    timestamp: Optional[str] = Field(None, description="Timestamp of the event (ISO format).")
    optional_information: Optional[str] = Field(None, description="Any other pertinent details or context.")
    source: Optional[str] = Field(None, description="Source of the event (e.g., WhatsApp, Twitter, News).")
    severity: Optional[Literal["P0", "P1", "P2", "P3", "P4", "P5"]] = Field(None, description="Severity of the event. Can be classified into 5 categories only P0, P1, P2, P3, P4, P5, where P0 is highest severity")
    sentiment: Optional[Literal[1, 0, -1]] = Field(None, description="Sentiment expressed in the event. Can be of only three categories: Positive is 1, Neutral is 0, Negative -1.")
