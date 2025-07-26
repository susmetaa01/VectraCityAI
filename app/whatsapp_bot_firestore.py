import os
import requests
import json
import uuid
from datetime import datetime, timedelta

from flask import Flask, request, jsonify
from twilio.rest import Client
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import firestore

app = Flask(__name__)

TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID', 'AC95b60f8ee2f0ed253bd27cd4a50d9dfa')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', '6215f2f4af9091e8ff996c791a36f0eb')
TWILIO_WHATSAPP_NUMBER = os.getenv('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886')

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'schrodingers-cat-466413')

PUBSUB_TOPIC_ID_INCOMING = os.getenv('PUBSUB_TOPIC_ID_INCOMING', 'whatsapp-incoming-raw-events')
PUBSUB_TOPIC_ID_TRIGGER = os.getenv('PUB_SUB_TOPIC_ID_TRIGGER', 'vectraCityAI-event-trigger')

# GCS Bucket for media files (remains the same)
GCS_MEDIA_BUCKET = os.getenv('GCS_MEDIA_BUCKET', 'vectra_city_ai-whatsapp-media')

# --- Initialize Google Cloud Clients ---
publisher = pubsub_v1.PublisherClient()
PUBSUB_TOPIC_PATH_INCOMING = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID_INCOMING)
PUBSUB_TOPIC_PATH_TRIGGER = publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID_TRIGGER)

storage_client = storage.Client(project=GCP_PROJECT_ID)
firestore_db = firestore.Client(project=GCP_PROJECT_ID)  # <-- NEW: Initialize Firestore client

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# --- Firestore Session Management Functions ---
FIRESTORE_COLLECTION_SESSIONS = 'whatsapp_sessions'  # Firestore collection name for sessions
LOCATION_TIMEOUT_MINUTES = 5


def get_session_from_firestore(from_number):
    """Retrieves session data from Firestore."""
    doc_ref = firestore_db.collection(FIRESTORE_COLLECTION_SESSIONS).document(
        from_number.replace('whatsapp:', ''))
    doc = doc_ref.get()
    if doc.exists:
        print(f"Session found in Firestore for {from_number}")
        return doc.to_dict()
    print(f"No active session found in Firestore for {from_number}")
    return None


def save_session_to_firestore(from_number, session_data):
    """Saves/updates session data in Firestore."""
    doc_ref = firestore_db.collection(FIRESTORE_COLLECTION_SESSIONS).document(
        from_number.replace('whatsapp:', ''))
    try:
        doc_ref.set(session_data)  # set() will create or overwrite
        print(f"Session saved to Firestore for {from_number}")
        return True
    except Exception as e:
        print(f"Error saving session to Firestore for {from_number}: {e}")
        return False


def delete_session_from_firestore(from_number):
    """Deletes session data from Firestore."""
    doc_ref = firestore_db.collection(FIRESTORE_COLLECTION_SESSIONS).document(
        from_number.replace('whatsapp:', ''))
    try:
        if doc_ref.get().exists:  # Check if document exists before trying to delete
            doc_ref.delete()
            print(f"Session deleted from Firestore for {from_number}")
            return True
        return False
    except Exception as e:
        print(f"Error deleting session from Firestore for {from_number}: {e}")
        return False


# --- Pub/Sub Publishing Functions (remain the same) ---
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

def build_pubsub_payload(payload):
    return json.dumps(payload)


def publish_trigger_event_to_pubsub(payload):
    """Publishes a trigger event (just SID) to the trigger topic."""
    try:
        data_json = build_pubsub_payload(payload)
        future = publisher.publish(PUBSUB_TOPIC_PATH_TRIGGER, data_json.encode('utf-8'))
        print(
            f"Published trigger event to {PUBSUB_TOPIC_ID_TRIGGER} with ID: {future.result()} for SID: {payload['message_sid']}")
        return True
    except Exception as e:
        print(f"Failed to publish trigger event to Pub/Sub: {e}")
        return False


# --- Helper Function to Send Outbound WhatsApp Messages (remains the same) ---
def send_whatsapp_message(to_number, text_message):
    try:
        message = twilio_client.messages.create(
            from_=TWILIO_WHATSAPP_NUMBER,
            body=text_message,
            to=to_number
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
    message_sid = request.form.get('MessageSid')  # SID of the incoming message
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
    # Print what's in Firestore session for debugging
    current_firestore_session = get_session_from_firestore(from_number)
    print(f"Firestore Session for {from_number}: {current_firestore_session}")
    print(f"-------------------------------")

    # Determine incoming message type
    incoming_type = 'unknown'
    if message_body:
        incoming_type = 'text'
    elif num_media > 0:
        incoming_type = 'media'
    elif latitude and longitude:
        incoming_type = 'location'

    print(f"INCOMING TYPE: {incoming_type}")

    # --- Handle Incoming Location Messages ---
    if incoming_type == 'location':
        session_data = get_session_from_firestore(from_number)  # Retrieve session from Firestore
        print(f"SESSION DATA: {session_data}")

        if session_data:
            original_payload = session_data['payload']
            waiting_since_str = session_data['waiting_since']  # Timestamp is stored as string

            try:
                waiting_since = datetime.fromisoformat(waiting_since_str)
            except ValueError:
                print(
                    f"Warning: Could not parse waiting_since timestamp: {waiting_since_str}. Assuming recent.")
                waiting_since = current_time  # Fallback

            if current_time - waiting_since <= timedelta(minutes=LOCATION_TIMEOUT_MINUTES):
                # Location received within timeout, enrich and publish original event

                # Add location details to the payload
                original_payload['latitude'] = latitude
                original_payload['longitude'] = longitude
                original_payload['address'] = address
                original_payload['label'] = label
                original_payload['location_resolved'] = True

                print(f"LATITUDE XX : {latitude}")
                print(f"LONGITUDE XX : {longitude}")

                # If original message was media, upload it to GCS NOW (using its temp GCS URI from session)
                if original_payload.get(
                        'message_type') == 'media_pending_location':  # Check for pending media type
                    media_gcs_uri_temp = session_data.get('media_gcs_uri_temp')
                    media_content_type_from_session = session_data.get(
                        'media_content_type_from_session')

                    if media_gcs_uri_temp and media_content_type_from_session:
                        try:
                            # Re-fetch blob to rename/move it, or simply use the temp URI as final
                            # For simplicity, we'll assume the temp GCS URI can be the final one,
                            # or you'd move/copy it to a more organized path here.
                            # The temporary file would eventually be cleaned by GCS lifecycle management.

                            original_payload[
                                'media_gcs_uri'] = media_gcs_uri_temp  # Set the final GCS URI
                            original_payload['media_content_type'] = media_content_type_from_session

                            # Update message_type from 'media_pending_location' to actual type
                            if media_content_type_from_session.startswith('image/'):
                                original_payload['message_type'] = 'image'
                            elif media_content_type_from_session.startswith('video/'):
                                original_payload['message_type'] = 'video'
                            else:
                                original_payload['message_type'] = 'other_media'
                            print(
                                f"  Media GCS URI resolved for publishing: {original_payload['media_gcs_uri']}")

                        except Exception as e:
                            print(f"  Error resolving media GCS URI from session: {e}")
                            original_payload['error'] = original_payload.get('error',
                                                                             '') + f"Failed to resolve media GCS URI from session: {e}"
                            original_payload['media_gcs_uri'] = None  # Clear if error
                            original_payload['media_content_type'] = None

                print(
                    f"Location received for pending message {original_payload.get('message_sid')}. Publishing to main topic.")
                print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                print(f"original_payload: {original_payload}")
                print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
                # Publish the full, enriched event to the main Pub/Sub topic
                if publish_full_event_to_pubsub(original_payload):
                    # If full event published successfully, send trigger event and delete session
                    publish_trigger_event_to_pubsub(original_payload)
                    delete_session_from_firestore(from_number)  # Clear session from Firestore

                send_whatsapp_message(from_number,
                                      "Thanks! We've received your location and are processing your report. Please allow a moment for analysis.")

            else:
                # Location received, but original message timed out
                print(
                    f"Location received for message {original_payload.get('message_sid')} but it already timed out.")
                send_whatsapp_message(from_number,
                                      "Thanks for your location. Your previous message timed out as location wasn't provided promptly, so it was discarded. Please send your report again with location if needed.")
                # Clear session from Firestore
                delete_session_from_firestore(from_number)
                # Option: Publish the *standalone* location as a separate event if you want to record it
                publish_full_event_to_pubsub({
                    'from_number': from_number,
                    'message_sid': message_sid,  # This is the location message's SID
                    'message_type': 'standalone_location',
                    'timestamp': request.form.get('SmsReceivedTime', None),
                    'latitude': latitude, 'longitude': longitude, 'address': address, 'label': label
                })
        else:
            # Standalone location message, no pending request found
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
        session_data = get_session_from_firestore(from_number)

        # If there's an existing session, check for timeout
        if session_data:
            waiting_since_str = session_data['waiting_since']
            try:
                waiting_since = datetime.fromisoformat(waiting_since_str)
            except ValueError:
                print(
                    f"Warning: Could not parse waiting_since timestamp from existing session: {waiting_since_str}. Assuming recent for timeout check.")
                waiting_since = current_time  # Fallback for corrupted timestamp

            if current_time - waiting_since > timedelta(minutes=LOCATION_TIMEOUT_MINUTES):
                print(f"Discarding old pending request from {from_number} due to timeout.")
                delete_session_from_firestore(from_number)  # Clear old timed-out session
                send_whatsapp_message(from_number,
                                      "Your previous message/image was discarded as location wasn't provided in time. Please send your report again.")
            else:
                # User sent a new message while still waiting for location for a previous one
                print(
                    f"User {from_number} sent new message while still waiting for location for previous. Reminding to send location first.")
                send_whatsapp_message(from_number,
                                      "Please provide your location for your *previous* message first. If you want to send a new report, please do so after sharing location for the current pending request, or after it times out.")
                return jsonify({'status': 'ok'}), 200  # Acknowledge and return early

        # Create a new payload for Pub/Sub (will be stored in Firestore, not published yet)
        payload_for_pubsub = {
            'from_number': from_number,
            'message_sid': message_sid,
            'message_type': incoming_type,
            # 'text' or 'media' (will be updated to image/video later if location received)
            'timestamp': request.form.get('SmsReceivedTime', None),
            'text_body': message_body,
            'media_gcs_uri': None,
            # GCS URI will be added IF location received and media is applicable
            'media_content_type': None,
            # Content type will be added IF location received and media is applicable
            'latitude': None,
            'longitude': None,
            'address': None,
            'label': None,
            'location_resolved': False
        }

        media_gcs_uri_temp = None  # GCS URI of temporary media storage
        media_content_type_temp = None

        if incoming_type == 'media':
            media_url = request.form.get('MediaUrl0')
            media_content_type_temp = request.form.get('MediaContentType0')

            if media_url and media_content_type_temp:
                print(f"  Attempting to download media from: {media_url}")
                try:
                    media_response = requests.get(media_url,
                                                  auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN))
                    media_response.raise_for_status()
                    media_bytes = media_response.content

                    ext = media_content_type_temp.split('/')[-1]
                    if ext == 'jpeg': ext = 'jpg'

                    # Generate a unique filename for GCS temp storage
                    gcs_temp_filename = f"whatsapp_media_temp/{from_number.replace('whatsapp:', '')}_{message_sid}_{uuid.uuid4()}.{ext}"
                    bucket = storage_client.bucket(
                        GCS_MEDIA_BUCKET)  # Using the same media bucket, but different prefix
                    blob = bucket.blob(gcs_temp_filename)

                    blob.upload_from_string(media_bytes, content_type=media_content_type_temp)
                    media_gcs_uri_temp = f"gs://{GCS_MEDIA_BUCKET}/{gcs_temp_filename}"
                    print(f"  Media uploaded to GCS temp location: {media_gcs_uri_temp}")

                    # Update initial payload for session storage
                    payload_for_pubsub[
                        'message_type'] = 'media_pending_location'  # Mark as pending media
                    payload_for_pubsub['media_gcs_uri'] = media_gcs_uri_temp  # Store temp URI here
                    payload_for_pubsub['media_content_type'] = media_content_type_temp

                except requests.exceptions.RequestException as e:
                    print(f"  Error downloading media from Twilio: {e}")
                    payload_for_pubsub['error'] = f"Failed to download media: {e}"
                    payload_for_pubsub[
                        'message_type'] = 'text' if message_body else 'unknown'  # Revert type if media fails
                except Exception as e:
                    print(f"  Error uploading media to GCS temp: {e}")
                    payload_for_pubsub['error'] = f"Error uploading media to GCS temp: {e}"
                    payload_for_pubsub[
                        'message_type'] = 'text' if message_body else 'unknown'  # Revert type if media fails
            else:
                print("  Received media but no URL or content type found.")
                payload_for_pubsub['message_type'] = 'unsupported_media'
                payload_for_pubsub['error'] = "Unsupported media type or no URL."

        # Store the payload and timestamp in Firestore
        session_data_to_store = {
            'payload': payload_for_pubsub,
            'waiting_since': current_time.isoformat(),  # Store as ISO format string
            'media_gcs_uri_temp': media_gcs_uri_temp,  # Store temp GCS URI in session data
            'media_content_type_from_session': media_content_type_temp
            # Content type for temp media
        }
        save_session_to_firestore(from_number, session_data_to_store)

        send_whatsapp_message(from_number,
                              "Thanks for your report! To help us pinpoint the issue, please share your **current location** via WhatsApp's attachment icon (Location). You have 5 minutes to share it before this report is discarded.")

    # --- Handle Unsupported Message Types ---
    else:
        print(f"Received unsupported message type from {from_number}: {request.form}")
        send_whatsapp_message(from_number,
                              "Sorry, I can only process text, images, videos, or shared locations. Please try again.")

    return jsonify({'status': 'ok'}), 200  # Always return 200 OK to Twilio quickly


# --- Run the Flask App ---
if __name__ == '__main__':
    print(f"Flask app starting... Listening for Twilio webhooks on /twilio-webhook")
    print(f"Location timeout set to {LOCATION_TIMEOUT_MINUTES} minutes.")
    print(
        f"NOTE: For production, you'll need a separate Cloud Function or scheduled job to clean up timed-out sessions in Firestore based on 'waiting_since' timestamp.")

    app.run(host='0.0.0.0', port=5000, debug=True)  # debug=True is good for dev, remove for prod
