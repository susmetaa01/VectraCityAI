#!/bin/bash

# Source your virtual environment
source venv/bin/activate

# Set environment variables
export TWILIO_ACCOUNT_SID="ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
export TWILIO_AUTH_TOKEN="your_twilio_auth_token"
export TWILIO_WHATSAPP_NUMBER="whatsapp:+14155238886"

# ... add all other environment variables here ...
export GCP_PROJECT_ID="schrodingers-cat-466413"
export TWITTER_BEARER_TOKEN="YOUR_TWITTER_BEARER_TOKEN"
# ... etc.

# Run your desired Python script
python app/whatsapp_bot_firestore.py

# You could also run the Beam pipeline similarly:
python -m src.vectra_city_pipeline --runner=DirectRunner --streaming