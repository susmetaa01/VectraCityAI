from typing import Optional

import uvicorn
from fastapi import FastAPI, APIRouter, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from gemini_client import analyze_with_gemini
from models import AnalysisResponse, InfoInput, Geolocation

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()

SYSTEM_PROMPT = (
    "Your a urban intelligence analyser. You are responsible for analyzing images, text along with geolocation information provided. Given the input do following.\n"
    "1. Categorise the input data provided into one or more of the following categories. Also give relevancy score to it between 0 and 1. Dont categorise anything which is less than 0.5 relevancy.\n"
    "- Road complaints\n- Power or electricity outage\n- Mob immobilisation\n- Heavy traffic congestion\n- Medical and medical requirements\n- Bomb threat\n"
    "2. Generate 2-3 subcategory from the above categories and classify the input.\n"
    "3. Classify to one or more of the following department with relevancy score. Dont classify if score below 0.75\n"
    "- Municipality\n- Police\n- Ambulance\n- Traffic police\n- Fire station\n"
    "Also provide summary of the input provided. Include information from image if given."
)


@router.post("/analyze", response_model=AnalysisResponse)
async def analyze(
        image: Optional[UploadFile] = File(None),
        information: Optional[str] = Form(None),
        latitude: Optional[str] = Form(None),
        longitude: Optional[str] = Form(None),
        area: Optional[str] = Form(None)
):
    # Validate input
    if not ((image and latitude and longitude) or (information and latitude and longitude)):
        raise HTTPException(status_code=400,
                            detail="Either (image + geolocation) or (information + geolocation) must be provided.")

    geolocation = Geolocation(latitude=latitude, longitude=longitude, area=area)
    image_bytes = await image.read() if image else None
    result = await analyze_with_gemini(
        SYSTEM_PROMPT, image_bytes, information, geolocation)
    return JSONResponse(content=result)


@router.post("/analyze-json", response_model=AnalysisResponse)
async def analyze_json(input: InfoInput):
    result = await analyze_with_gemini(
        SYSTEM_PROMPT, None, input.information, input.geoLocation, input.optional_information
    )
    return JSONResponse(content=result)


app.include_router(router, prefix="/api")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)