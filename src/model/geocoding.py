import logging
import os
from typing import Dict, Any

import googlemaps
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
logger = logging.getLogger('uvicorn.error')


class GeocodingError(Exception):
    """Custom exception for geocoding errors"""
    pass


def forward_geocode(area: str, sublocation: str = None) -> Dict[str, Any]:
    """
    Performs forward geocoding using Google Maps Platform API to get coordinates from area and sublocation.

    Args:
        area (str): Area/city name
        sublocation (str, optional): Sublocation/neighborhood name

    Returns:
        Dict[str, Any]: JSON response containing latitude and longitude information

    Raises:
        GeocodingError: If geocoding fails or API key is missing
    """

    # Get API key from environment variables
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    if not api_key:
        raise GeocodingError("Google Maps API key not found. Please set GOOGLE_MAPS_API_KEY environment variable.")

    try:
        # Initialize Google Maps client
        gmaps = googlemaps.Client(key=api_key)

        # Build address string
        address_parts = []
        if sublocation:
            address_parts.append(sublocation)
        if area:
            address_parts.append(area)

        address = ", ".join(address_parts)

        logger.info(f"Performing forward geocoding for address: {address}")
        geocode_result = gmaps.geocode(address)

        if not geocode_result:
            return {
                "success": False,
                "error": "No results found for the given address",
                "area": area,
                "sublocation": sublocation,
                "latitude": None,
                "longitude": None
            }

        # Extract coordinates from the first result
        result = geocode_result[0]
        geometry = result.get('geometry', {})
        location = geometry.get('location', {})

        latitude = location.get('lat')
        longitude = location.get('lng')

        # Prepare response
        response = {
            "success": True,
            "area": area,
            "sublocation": sublocation,
            "latitude": latitude,
            "longitude": longitude,
            "formatted_address": result.get('formatted_address', ''),
            "raw_result": result
        }

        return response

    except googlemaps.exceptions.ApiError as e:
        raise GeocodingError(f"Google Maps API error: {str(e)}")
    except googlemaps.exceptions.HTTPError as e:
        raise GeocodingError(f"HTTP error while calling Google Maps API: {str(e)}")
    except googlemaps.exceptions.Timeout as e:
        raise GeocodingError(f"Timeout error while calling Google Maps API: {str(e)}")
    except Exception as e:
        raise GeocodingError(f"Unexpected error during forward geocoding: {str(e)}")

def reverse_geocode(latitude: float, longitude: float) -> Dict[str, Any]:
    """
    Performs reverse geocoding using Google Maps Platform API to get area and sublocation.

    Args:
        latitude (float): Latitude coordinate
        longitude (float): Longitude coordinate

    Returns:
        Dict[str, Any]: JSON response containing area and sublocation information

    Raises:
        GeocodingError: If geocoding fails or API key is missing
    """

    # Get API key from environment variables
    api_key = os.getenv('GOOGLE_MAPS_API_KEY')
    if not api_key:
        raise GeocodingError("Google Maps API key not found. Please set GOOGLE_MAPS_API_KEY environment variable.")

    try:
        # Initialize Google Maps client
        gmaps = googlemaps.Client(key=api_key)

        # Perform reverse geocoding
        logger.info(f"Performing reverse geocoding for latitude={latitude}, and longitude={longitude}")
        reverse_geocode_result = gmaps.reverse_geocode((latitude, longitude))

        if not reverse_geocode_result:
            return {
                "success": False,
                "error": "No results found for the given coordinates",
                "latitude": latitude,
                "longitude": longitude,
                "area": None,
                "sublocation": None
            }

        # Extract location information from the first result
        result = reverse_geocode_result[0]
        address_components = result.get('address_components', [])

        # Initialize variables to store extracted information
        area = None
        sublocation = None
        administrative_area = None
        country = None
        postal_code = None

        # Parse address components to extract relevant information
        for component in address_components:
            types = component.get('types', [])
            long_name = component.get('long_name', '')

            # Extract area (locality/city)
            if 'locality' in types:
                area = long_name
            elif 'administrative_area_level_2' in types and not area:
                area = long_name
            elif 'administrative_area_level_1' in types and not area:
                administrative_area = long_name

            # Extract sublocation (neighborhood/sublocality)
            if 'sublocality' in types or 'sublocality_level_1' in types:
                sublocation = long_name
            elif 'neighborhood' in types and not sublocation:
                sublocation = long_name

            # Extract additional information
            if 'country' in types:
                country = long_name
            elif 'postal_code' in types:
                postal_code = long_name

        # If area is still None, use administrative area as fallback
        if not area and administrative_area:
            area = administrative_area

        # Get formatted address
        formatted_address = result.get('formatted_address', '')

        # Prepare response
        response = {
            "success": True,
            "latitude": latitude,
            "longitude": longitude,
            "area": area,
            "sublocation": sublocation,
            "formatted_address": formatted_address,
            "additional_info": {
                "country": country,
                "postal_code": postal_code,
                "administrative_area": administrative_area
            },
            "raw_result": result  # Include raw result for debugging/additional processing
        }

        return response

    except googlemaps.exceptions.ApiError as e:
        raise GeocodingError(f"Google Maps API error: {str(e)}")
    except googlemaps.exceptions.HTTPError as e:
        raise GeocodingError(f"HTTP error while calling Google Maps API: {str(e)}")
    except googlemaps.exceptions.Timeout as e:
        raise GeocodingError(f"Timeout error while calling Google Maps API: {str(e)}")
    except Exception as e:
        raise GeocodingError(f"Unexpected error during reverse geocoding: {str(e)}")


def get_location_info(latitude: float, longitude: float, include_raw: bool = False) -> Dict[str, Any]:
    """
    Simplified function to get just area and sublocation information.

    Args:
        latitude (float): Latitude coordinate
        longitude (float): Longitude coordinate
        include_raw (bool): Whether to include raw API response

    Returns:
        Dict[str, Any]: Simplified JSON response with area and sublocation
    """
    try:
        full_result = reverse_geocode(latitude, longitude)

        # Create simplified response
        simplified_response = {
            "success": full_result["success"],
            "latitude": latitude,
            "longitude": longitude,
            "area": full_result.get("area"),
            "sublocation": full_result.get("sublocation"),
            "formatted_address": full_result.get("formatted_address")
        }

        if include_raw:
            simplified_response["raw_result"] = full_result.get("raw_result")

        return simplified_response

    except GeocodingError as e:
        return {
            "success": False,
            "error": str(e),
            "latitude": latitude,
            "longitude": longitude,
            "area": None,
            "sublocation": None
        }
