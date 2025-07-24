import logging
from typing import Optional, List

from pydantic import BaseModel, model_validator, Field

from analyze.geocoding import reverse_geocode, GeocodingError

logger = logging.getLogger('uvicorn.error')

class Geolocation(BaseModel):
    latitude: float = Field(..., description="Latitude coordinate")
    longitude: float = Field(..., description="Longitude coordinate")
    area: Optional[str] = Field(None, description="Area/city information (auto-populated)")
    sublocation: Optional[str] = Field(None, description="Sublocation/neighborhood information (auto-populated)")
    address: Optional[str] = Field(None, description="Formatted address (auto-populated)")

    @model_validator(mode='after')
    def populate_location_info(self):
        """Automatically populate area, sublocation, and address using reverse geocoding."""
        # Only perform reverse geocoding if the location fields are not already populated
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

        return self

    def __str__(self) -> str:
        """String representation for use in prompts and logging."""
        parts = [f"Latitude: {self.latitude}", f"Longitude: {self.longitude}"]
        if self.area:
            parts.append(f"Area: {self.area}")
        if self.sublocation:
            parts.append(f"Sublocation: {self.sublocation}")
        if self.address:
            parts.append(f"Address: {self.address}")
        return ", ".join(parts)


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
    geoLocation: Geolocation
    problem: List[ProblemCategory]
    department: List[Department]


# Input model for JSON (info + geolocation)
class InfoInput(BaseModel):
    information: str
    geoLocation: Geolocation
    optional_information: Optional[str] = None


class DataInput(BaseModel):
    url: str
    mimeType: str


# New input model for JSON-based analyze endpoint
class AnalyzeInput(BaseModel):
    data: Optional[DataInput] = None  # URL to image
    information: Optional[str] = None  # Text information
    geolocation: Geolocation
    timestamp: Optional[str] = None