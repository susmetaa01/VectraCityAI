# src/model/incoming_events.py (or src/schemas.py)
import logging
from typing import Optional, List
from pydantic import BaseModel, Field # Ensure Field is imported if used elsewhere
# from pydantic import model_validator # No longer needed for Geolocation
from datetime import datetime

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

    # REMOVED: @model_validator(mode='after') and populate_location_info method.

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