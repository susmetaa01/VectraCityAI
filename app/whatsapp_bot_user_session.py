import os
import requests
import json
import uuid
from datetime import datetime, timedelta

from flask import Flask, request, jsonify
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client
from google.cloud import pubsub_v1
from google.cloud import storage

# --- Configuration ---
app = Flask(__name__)

TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID', 'AC95b60f8ee2f0ed253bd27cd4a50d9dfa')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', '6215f2f4af9091e8ff996c791a36f0eb')
TWILIO_WHATSAPP_NUMBER = os.getenv('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886')

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'schrodingers-cat-466413')
# Main topic for full, enriched message payloads
PUBSUB_TOPIC_ID_INCOMING = os.getenv('PUBSUB_TOPIC_ID_INCOMING', 'whatsapp-incoming-raw-events')
# NEW: Topic for just SIDs, acting as a trigger
PUBSUB_TOPIC_ID_TRIGGER = os.getenv('PUBSUB_TOPIC_ID_TRIGGER', 'whatsapp-event-triggers')
GCS_MEDIA_BUCKET = os.getenv('GCS_MEDIA_BUCKET', 'vectra_city_ai-whatsapp-media')

publisher = pubsub_v1.PublisherClient()
PUBSUB_TOPIC_PATH_INCOMING = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID_INCOMING)
PUBSUB_TOPIC_PATH_TRIGGER = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID_TRIGGER) # NEW Trigger topic path

# Initialize GCS client with explicit project ID
storage_client = storage.Client(project=GCP_PROJECT_ID)

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# --- Conversation State Storage (IN-MEMORY - NOT PRODUCTION READY) ---
# This dictionary will store pending messages awaiting location, keyed by from_number
# Structure: { 'whatsapp:+1234567890': { 'original_payload': {...}, 'waiting_since': datetime_object, 'media_bytes': b'', 'media_content_type': '' } }
user_sessions = {}
LOCATION_TIMEOUT_MINUTES = 5

# --- Pub/Sub Publishing Functions ---
def publish_full_event_to_pubsub(payload_dict):
    """Publishes a full event payload to the main incoming topic."""
    try:
        data_json = json.dumps(payload_dict)
        future = publisher.publish(PUBSUB_TOPIC_PATH_INCOMING, data_json.encode('utf-8'))
        print(f"Published full event to {PUBSUB_TOPIC_ID_INCOMING} with ID: {future.result()}")
        return True
    except Exception as e:
        print(f"Failed to publish full event to Pub/Sub: {e}")
        return False

def publish_trigger_event_to_pubsub(message_sid):
    """Publishes a trigger event (just SID) to the trigger topic."""
    try:
        payload = {'message_sid': message_sid, 'event_type': 'message_processed_and_stored', 'timestamp': datetime.now().isoformat()}
        data_json = json.dumps(payload)
        future = publisher.publish(PUBSUB_TOPIC_PATH_TRIGGER, data_json.encode('utf-8'))
        print(f"Published trigger event to {PUBSUB_TOPIC_ID_TRIGGER} with ID: {future.result()} for SID: {message_sid}")
        return True
    except Exception as e:
        print(f"Failed to publish trigger event to Pub/Sub: {e}")
        return False

# --- Helper Function to Send Outbound WhatsApp Messages ---
def send_whatsapp_message(to_number, text_message):
    try:
        message = twilio_client.messages.create(
            from_=TWILIO_WHATSAPP_NUMBER,
            body=text_message,
            to=f"whatsapp:{to_number}"
        )
        print(f"Message sent successfully to {to_number}. SID: {message.sid}")
        return True
    except Exception as e:
        print(f"Error sending message to {to_number}: {e}")
        return False

# --- Main Webhook Handler ---
@app.route('/twilio-webhook', methods=['POST'])
def handle_twilio_message():
    from_number = request.form.get('From')
    message_sid = request.form.get('MessageSid') # SID of the incoming message
    message_body = request.form.get('Body')
    num_media = int(request.form.get('NumMedia', 0))

    latitude = request.form.get('Latitude')
    longitude = request.form.get('Longitude')
    address = request.form.get('Address')
    label = request.form.get('Label')

    current_time = datetime.now()

    print(f"--- Received Twilio Webhook ---")
    print(f"Message SID: {message_sid}")
    print(f"From: {from_number}")
    print(f"Body: {message_body if message_body else '[No text body]'}")
    print(f"NumMedia: {num_media}")
    print(f"Location: Lat={latitude}, Lon={longitude}, Address={address}, Label={label}")
    print("Full Form Data:", request.form)
    print(f"Current User Sessions: {user_sessions.keys()}")
    print(f"-------------------------------")

    # Determine incoming message type
    incoming_type = 'unknown'
    if message_body:
        incoming_type = 'text'
    elif num_media > 0:
        incoming_type = 'media'
    elif latitude and longitude:
        incoming_type = 'location'


    # --- Handle Incoming Location Messages ---
    if incoming_type == 'location':
        if from_number in user_sessions:
            session_data = user_sessions.pop(from_number) # Remove session as we're resolving it
            original_payload = session_data['payload']
            waiting_since = session_data['waiting_since']
            media_bytes_to_upload = session_data['media_bytes']
            media_content_type_to_upload = session_data['media_content_type']

            if current_time - waiting_since <= timedelta(minutes=LOCATION_TIMEOUT_MINUTES):
                # Location received within timeout, enrich and publish original event

                # Add location details to the payload
                original_payload['latitude'] = latitude
                original_payload['longitude'] = longitude
                original_payload['address'] = address
                original_payload['label'] = label
                original_payload['location_resolved'] = True

                # If original message was media, upload it to GCS NOW
                if original_payload.get('message_type') == 'media' and media_bytes_to_upload:
                    try:
                        ext = media_content_type_to_upload.split('/')[-1]
                        if ext == 'jpeg': ext = 'jpg'
                        gcs_filename = f"whatsapp_media/{from_number.replace('whatsapp:', '')}_{original_payload['message_sid']}_{uuid.uuid4()}.{ext}"
                        bucket = storage_client.bucket(GCS_MEDIA_BUCKET)
                        blob = bucket.blob(gcs_filename)
                        blob.upload_from_string(media_bytes_to_upload, content_type=media_content_type_to_upload)
                        original_payload['media_gcs_uri'] = f"gs://{GCS_MEDIA_BUCKET}/{gcs_filename}"
                        print(f"  Delayed media upload to GCS successful: {original_payload['media_gcs_uri']}")

                        # Update message_type if it was generic 'media' and now resolved with location context
                        if media_content_type_to_upload.startswith('image/'):
                            original_payload['message_type'] = 'image'
                        elif media_content_type_to_upload.startswith('video/'):
                            original_payload['message_type'] = 'video'
                        else:
                            original_payload['message_type'] = 'other_media'

                    except Exception as e:
                        print(f"  Error during delayed media upload: {e}")
                        original_payload['error'] = original_payload.get('error', '') + f"Failed delayed media upload: {e}"
                        # If upload fails, message type might default to 'text' if body was present or 'unknown'

                print(f"Location received for pending message {original_payload.get('message_sid')}. Publishing to main topic.")
                # Publish the full, enriched event to the main Pub/Sub topic
                if publish_full_event_to_pubsub(original_payload):
                    # If full event published successfully, send trigger event
                    publish_trigger_event_to_pubsub(original_payload['message_sid'])

                send_whatsapp_message(from_number, "Thanks! We've received your location and are processing your report. Please allow a moment for analysis.")

            else:
                # Location received, but original message timed out
                print(f"Location received for message {original_payload.get('message_sid')} but it already timed out.")
                send_whatsapp_message(from_number, "Thanks for your location. Your previous message timed out as location wasn't provided promptly, so it was discarded. Please send your report again with location if needed.")
                # Option: Publish the *standalone* location as a separate event if you want to record it
                publish_full_event_to_pubsub({
                    'from_number': from_number,
                    'message_sid': message_sid, # This is the location message's SID
                    'message_type': 'standalone_location',
                    'timestamp': request.form.get('SmsReceivedTime', None),
                    'latitude': latitude, 'longitude': longitude, 'address': address, 'label': label
                })
        else:
            # Standalone location message, no pending request
            print(f"Received standalone location from {from_number}. No active session to link.")
            payload_for_pubsub = {
                'from_number': from_number,
                'message_sid': message_sid,
                'message_type': 'location',
                'timestamp': request.form.get('SmsReceivedTime', None),
                'latitude': latitude, 'longitude': longitude, 'address': address, 'label': label
            }
            # Publish standalone location to main topic
            if publish_full_event_to_pubsub(payload_for_pubsub):
                publish_trigger_event_to_pubsub(message_sid)
            send_whatsapp_message(from_number, "Thanks for sharing your location!")

    # --- Handle New Text/Image/Video Messages (Initiating a new request for location) ---
    elif incoming_type == 'text' or incoming_type == 'media':
        # Check if there's an existing pending request from this user that timed out
        if from_number in user_sessions:
            session_data = user_sessions[from_number]
            waiting_since = session_data['waiting_since']
            if current_time - waiting_since > timedelta(minutes=LOCATION_TIMEOUT_MINUTES):
                print(f"Discarding old pending request from {from_number} due to timeout.")
                user_sessions.pop(from_number) # Clear old timed-out session
                send_whatsapp_message(from_number, "Your previous message/image was discarded as location wasn't provided in time. Please send your report again.")
            else:
                # User sent a new message while still waiting for location for a previous one
                print(f"User {from_number} sent new message while still waiting for location for previous. Reminding to complete first.")
                send_whatsapp_message(from_number, "Please provide your location for your *previous* message first. If you want to send a new report, please do so after sharing location for the current pending request, or after it times out.")
                return jsonify({'status': 'ok'}), 200 # Acknowledge and return early

        # Create a new payload for Pub/Sub, initially without location
        payload_for_pubsub = {
            'from_number': from_number,
            'message_sid': message_sid,
            'message_type': incoming_type, # 'text' or 'media' (will be updated to image/video later if location received)
            'timestamp': request.form.get('SmsReceivedTime', None),
            'text_body': message_body,
            'media_gcs_uri': None, # GCS URI will be added IF location received and media is applicable
            'media_content_type': None, # Content type will be added IF location received and media is applicable
            'latitude': None,
            'longitude': None,
            'address': None,
            'label': None,
            'location_resolved': False # Flag to indicate if location was added later
        }

        media_bytes_to_store = b''
        media_content_type_to_store = ''

        if incoming_type == 'media':
            media_url = request.form.get('MediaUrl0')
            media_content_type_to_store = request.form.get('MediaContentType0')

            if media_url and media_content_type_to_store:
                print(f"  Attempting to download media from: {media_url}")
                try:
                    media_response = requests.get(media_url, auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
                    media_response.raise_for_status()
                    media_bytes_to_store = media_response.content
                    print(f"  Media bytes downloaded. Size: {len(media_bytes_to_store)/1024:.2f} KB.")
                except requests.exceptions.RequestException as e:
                    print(f"  Error downloading media from Twilio: {e}")
                    payload_for_pubsub['error'] = f"Failed to download media: {e}"
            else:
                print("  Received media but no URL or content type found.")
                payload_for_pubsub['message_type'] = 'unsupported_media'
                payload_for_pubsub['error'] = "Unsupported media type or no URL."

        # Store the payload, timestamp, and downloaded media bytes (if any)
        user_sessions[from_number] = {
            'payload': payload_for_pubsub,
            'waiting_since': current_time,
            'media_bytes': media_bytes_to_store, # Store actual bytes for delayed GCS upload
            'media_content_type': media_content_type_to_store
        }
        send_whatsapp_message(from_number, "Thanks for your report! To help us pinpoint the issue, please share your **current location** via WhatsApp's attachment icon (Location). You have 5 minutes to share it before this report is discarded.")

    # --- Handle Unsupported Message Types ---
    else:
        print(f"Received unsupported message type from {from_number}: {request.form}")
        send_whatsapp_message(from_number, "Sorry, I can only process text, images, videos, or shared locations. Please try again.")


    return jsonify({'status': 'ok'}), 200 # Always return 200 OK to Twilio quickly

# --- Run the Flask App ---
if __name__ == '__main__':
    # Set these environment variables before running, or hardcode for testing:
    # export TWILIO_ACCOUNT_SID='ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    # export TWILIO_AUTH_TOKEN='your_twilio_auth_token'
    # export TWILIO_WHATSAPP_NUMBER='whatsapp:+14155238886'
    # export GCP_PROJECT_ID='your-gcp-project-id'
    # export PUBSUB_TOPIC_ID_INCOMING='whatsapp-incoming-raw-events'
    # export PUBSUB_TOPIC_ID_TRIGGER='whatsapp-event-triggers' # NEW Topic
    # export GCS_MEDIA_BUCKET='your-whatsapp-media-bucket'

    print(f"Flask app starting... Listening for Twilio webhooks on /twilio-webhook")
    print(f"Location timeout set to {LOCATION_TIMEOUT_MINUTES} minutes.")
    print(f"WARNING: This in-memory state management is NOT suitable for production deployment (e.g., Cloud Run, App Engine) due to ephemeral state across instances/restarts).")
    print(f"Consider using Google Cloud Firestore or Memorystore (Redis) for robust session management in production.")

    app.run(host='0.0.0.0', port=5000, debug=True) # debug=True is good for dev, remove for prod