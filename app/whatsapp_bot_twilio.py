import os
import requests
import json
import uuid

from flask import Flask, request, jsonify
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client
from google.cloud import pubsub_v1
from google.cloud import storage # <-- NEW: For GCS upload


app = Flask(__name__)

TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID', 'AC95b60f8ee2f0ed253bd27cd4a50d9dfa')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', '6215f2f4af9091e8ff996c791a36f0eb')
TWILIO_WHATSAPP_NUMBER = os.getenv('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886')

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'schrodingers-cat-466413')
# This topic receives raw event metadata + GCS URI for media
PUBSUB_TOPIC_ID_INCOMING = os.getenv('PUBSUB_TOPIC_ID_INCOMING', 'whatsapp-incoming-raw-events')
GCS_MEDIA_BUCKET = os.getenv('GCS_MEDIA_BUCKET', 'vectra_city_ai-whatsapp-media')

publisher = pubsub_v1.PublisherClient()
PUBSUB_TOPIC_PATH_INCOMING = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID_INCOMING)

storage_client = storage.Client() # Initialize GCS client

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


# --- Pub/Sub Publishing Function ---
def publish_to_pubsub(payload_dict):
    """
    Publishes a dictionary payload to the designated Pub/Sub topic.
    """
    try:
        data_json = json.dumps(payload_dict)
        future = publisher.publish(PUBSUB_TOPIC_PATH_INCOMING, data_json.encode('utf-8'))
        print(f"Published message to Pub/Sub with ID: {future.result()}")
        return True
    except Exception as e:
        print(f"Failed to publish to Pub/Sub: {e}")
        return False

# --- Handle Incoming Messages (POST request from Twilio Webhook) ---
@app.route('/twilio-webhook', methods=['POST'])
def handle_twilio_message():
    message_sid = request.form.get('MessageSid')
    from_number = request.form.get('From')
    message_body = request.form.get('Body')
    num_media = int(request.form.get('NumMedia', 0))

    print(f"--- Received Twilio Webhook ---")
    print(f"Message SID: {message_sid}")
    print(f"From: {from_number}")
    print(f"Body: {message_body if message_body else '[No text body]'}")
    print(f"NumMedia: {num_media}")
    print("Full Form Data:", request.form)
    print(f"-------------------------------")

    payload_for_pubsub = {
        'from_number': from_number,
        'message_sid': message_sid,
        'message_type': 'text', # Default type
        'timestamp': request.form.get('SmsReceivedTime', None),
        'text_body': message_body,
        'media_gcs_uri': None, # <-- NEW: To store GCS URI of media
        'media_content_type': None
    }

    if num_media > 0:
        media_url = request.form.get('MediaUrl0') # Twilio's temporary URL
        media_content_type = request.form.get('MediaContentType0')

        if media_url and media_content_type:
            print(f"  Attempting to download media from: {media_url}")
            try:
                # Download the media bytes from Twilio
                media_response = requests.get(media_url, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
                media_response.raise_for_status()
                media_bytes = media_response.content

                # Determine file extension
                ext = media_content_type.split('/')[-1]
                if ext == 'jpeg': ext = 'jpg' # Common mapping

                # Generate a unique filename for GCS
                gcs_filename = f"whatsapp_media/{from_number.replace('whatsapp:', '')}_{message_sid}_{uuid.uuid4()}.{ext}"
                bucket = storage_client.bucket(GCS_MEDIA_BUCKET)
                blob = bucket.blob(gcs_filename)

                # Upload to GCS
                blob.upload_from_string(media_bytes, content_type=media_content_type)
                media_gcs_uri = f"gs://{GCS_MEDIA_BUCKET}/{gcs_filename}"

                payload_for_pubsub['media_gcs_uri'] = media_gcs_uri
                payload_for_pubsub['media_content_type'] = media_content_type

                if media_content_type.startswith('image/'):
                    payload_for_pubsub['message_type'] = 'image'
                    print(f"  Image uploaded to GCS: {media_gcs_uri}")
                elif media_content_type.startswith('video/'):
                    payload_for_pubsub['message_type'] = 'video'
                    print(f"  Video uploaded to GCS: {media_gcs_uri}")
                else:
                    payload_for_pubsub['message_type'] = 'other_media'
                    print(f"  Other media type uploaded to GCS: {media_gcs_uri}")


            except requests.exceptions.RequestException as e:
                print(f"  Error downloading media from Twilio: {e}")
                payload_for_pubsub['error'] = f"Failed to download media from Twilio: {e}"
            except Exception as e:
                print(f"  Error uploading media to GCS: {e}")
                payload_for_pubsub['error'] = f"Failed to upload media to GCS: {e}"
        else:
            print("  Received media but no URL or content type found.")
            payload_for_pubsub['message_type'] = 'unsupported_media'
            payload_for_pubsub['error'] = "Unsupported media type or no URL from Twilio."


    # Publish the entire payload to Pub/Sub
    published_successfully = publish_to_pubsub(payload_for_pubsub)

    if published_successfully:
        return jsonify({'status': 'ok', 'message': 'Message received and sent to processing queue.'}), 200
    else:
        return jsonify({'status': 'error', 'message': 'Failed to process message (Pub/Sub error).'}), 500

# --- Function to Send Outbound WhatsApp Messages (if needed from Flask side) ---
# This remains as previously defined if you use it.
def send_whatsapp_message(to_number, text_message):
    try:
        message = twilio_client.messages.create(
            from_=TWILIO_WHATSAPP_NUMBER,
            body=text_message,
            to=f"whatsapp:{to_number}"
        )
        print(f"Message sent successfully to {to_number}. SID: {message.sid}")
        return message.sid
    except Exception as e:
        print(f"Error sending message to {to_number}: {e}")
        return None
if __name__ == '__main__':
    print("Flask app starting... Listening for Twilio webhooks on /twilio-webhook")
    app.run(host='0.0.0.0', port=5000)


