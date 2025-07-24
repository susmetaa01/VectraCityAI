To Run locally
```
source VectraCityAIEnv/bin/activate
ngrok http 5000
python app/whatsapp_bot_twilio.py
python -m src.vectra_city_pipeline --runner=DirectRunner --streaming
chmod +x server_init.sh
./server_init.sh
python src/ingestor/twitter_stream_publisher.py
python src/ingestor/google_news_rss_publisher.py
```
