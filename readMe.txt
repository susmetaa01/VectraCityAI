To Run locally
```
source VectraCityAIEnv/bin/activate
ngrok http 5000
python app/whatsapp_bot_firestore.py
python -m src/vectra_city_pipeline --runner=DirectRunner --streaming
chmod +x server_init.sh
./server_init.sh
python ingest/twitter_stream_publisher.py
python ingest/google_news_rss_publisher.py
```
