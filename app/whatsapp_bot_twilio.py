import os
import twilio
from flask import Flask, request, jsonify
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client

app = Flask(__name__)

TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID', 'AC95b60f8ee2f0ed253bd27cd4a50d9dfa')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', '6215f2f4af9091e8ff996c791a36f0eb')
TWILIO_WHATSAPP_NUMBER = os.getenv('TWILIO_WHATSAPP_NUMBER', 'whatsapp:+14155238886')

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

@app.route('/twilio-webhook', methods=['POST'])
def handle_twilio_message():
    message_sid = request.form.get('MessageSid')
    from_number = request.form.get('From')
    message_body = request.form.get('Body')
    media_url = request.form.get('MediaUrl')

    print(f"--- Received Twilio Webhook ---")
    print(f"Message SID: {message_sid}")
    print(f"From: {from_number}")
    print(f"Body: {message_body if message_body else '[No text body]'}")
    print(f"Media URL: {media_url if media_url else '[No media]'}")
    print("Full Form Data:", request.form)
    print(f"-------------------------------")

    # Example: Simple processing (for demonstration, you'd send to Pub/Sub)
    if message_body:
        response_text = f"Received your text: '{message_body}'. Processing now!"
        # In your actual system, you'd publish to Pub/Sub here:
        # publish_to_pubsub(from_number, message_body, media_url, message_sid)
    elif media_url:
        response_text = f"Received your media from {media_url}. Analyzing it!"
        # publish_to_pubsub(from_number, message_body, media_url, message_sid)
    else:
        response_text = "Thanks for your message! (No text or media detected)"

    return jsonify({'status': 'ok'}), 200 # Acknowledge the webhook

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


