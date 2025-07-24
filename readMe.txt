To Run locally
```
source VectraCityAIEnv/bin/activate
ngrok http 5000
python app/whatsapp_bot_twilio.py
python src/vectra_city_pipeline.py --runner=DirectRunner --streaming
```