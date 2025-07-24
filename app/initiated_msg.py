from twilio.rest import Client

account_sid = 'AC95b60f8ee2f0ed253bd27cd4a50d9dfa'
auth_token = '6215f2f4af9091e8ff996c791a36f0eb'
client = Client(account_sid, auth_token)

message = client.messages.create(
    from_='whatsapp:+14155238886',
    content_sid='HXb5b62575e6e4ff6129ad7c8efe1f983e',
    content_variables='{"1":"12/1","2":"3pm"}',
    to='whatsapp:+917200819376'
)

print(message.sid)

#appt reminder
# account_sid = 'AC95b60f8ee2f0ed253bd27cd4a50d9dfa'
# auth_token = '6215f2f4af9091e8ff996c791a36f0eb'
# client = Client(account_sid, auth_token)
#
# message = client.messages.create(
#     from_='whatsapp:+14155238886',
#     content_sid='HXb5b62575e6e4ff6129ad7c8efe1f983e',
#     content_variables='{"1":"12/1","2":"3pm"}',
#     to='whatsapp:+917200819376'
# )
#
# print(message.sid)

