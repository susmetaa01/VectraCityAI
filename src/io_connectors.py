import apache_beam as beam
import json
from . import config # Relative import to access config.py

class ReadTriggerEvents(beam.PTransform):
    """
    A PTransform to read trigger events from the designated Pub/Sub subscription.
    These events are expected to be JSON strings representing message SIDs.
    """
    def expand(self, pcoll):
        pubsub_subscription_path = f"projects/{config.GCP_PROJECT_ID}/subscriptions/{config.PUBSUB_SUBSCRIPTION_NAME_TRIGGER}"
        return (
                pcoll
                | 'ReadFromTriggerPubSub' >> beam.io.ReadFromPubSub(subscription=pubsub_subscription_path)
                | 'DecodeAndParseTriggerJson' >> beam.Map(lambda element: json.loads(element.decode('utf-8')))
        )

class ReadTwitterFeed(beam.PTransform):
    """
    A PTransform to read raw Twitter feed data from the designated Pub/Sub subscription.
    These are expected to be raw tweet JSON payloads.
    """
    def expand(self, pcoll):
        pubsub_subscription_path = f"projects/{config.GCP_PROJECT_ID}/subscriptions/{config.PUBSUB_SUBSCRIPTION_NAME_TWITTER}"
        return (
                pcoll
                | 'ReadFromTwitterPubSub' >> beam.io.ReadFromPubSub(subscription=pubsub_subscription_path)
        )

class ReadGoogleNewsFeed(beam.PTransform):
    """
    A PTransform to read raw Google News RSS feed data from the designated Pub/Sub subscription.
    """
    def expand(self, pcoll):
        pubsub_subscription_path = f"projects/{config.GCP_PROJECT_ID}/subscriptions/{config.PUBSUB_SUBSCRIPTION_NAME_GNEWS}" # Assuming a sub is named 'raw-google-news-feed-sub'
        return (
                pcoll
                | 'ReadFromGoogleNewsPubSub' >> beam.io.ReadFromPubSub(subscription=pubsub_subscription_path)
        )

# Future: Add other IO connectors here, e.g., WriteToInfluxDB, ReadGoogleNews
# class WriteToInfluxDB(beam.PTransform):
#     def expand(self, pcoll):
#         # Your InfluxDB writing logic here, likely using a custom DoFn
#         return pcoll | 'WriteRecordsToInfluxDB' >> beam.ParDo(InfluxDBWriterFn())