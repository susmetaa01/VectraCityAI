import apache_beam as beam
import json
from apache_beam import io
from apache_beam.options.pipeline_options import PipelineOptions

# Mock for io_connectors.ReadGoogleNewsFeed for demonstration purposes
class ReadGoogleNewsFeed(beam.PTransform):
    def expand(self, pcoll):
        mock_payload_str = b'{"source": "Google News RSS", "feed_url": "https://news.google.com/rss/search?q=Bengaluru+protest&hl=en-IN&gl=IN&ceid=IN:en", "article_id": "CBMixwFBVV95cUxOVjc3TzZxRWhJYTBpc1hfeTc1V3Z4YXZqeTlGRmZVd1hrN21YQUk4Vkg5a183cGlOaGNBcDdKTElBdHhKTU9OVXE0c01MNjFUeFk2VWJieDk2TlVFWENYdUg0eHRia2d1Q01ZYUcwdno1WHJCa0JMeHRIVjNydlpBcmhPWXEyUl9uZ0thamhJOGNmQmtIQVBISU5FcGtPUFZHdTZ3ejN3azN5a0Y5TEVpZVYzSGstN1RCQmNFZnNoOW5ORlM2RnJ5Y2cw", "title": "Karnataka: Anganwadi workers to protest face recognition system, election duties on July 26 - Udayavani", "link": "https://news.google.com/rss/articles/CBMixwFBVV95cUxOVjc3T0ZxRWhJYTBpc1hfeTc1V3Z4YXZqeTlGRmZVd1hrN21YQUk4Vkg5a183cGlOaGNBcDdKTElBdHhKTU9OVXE0c01MNjFUeFk2VWJieDk2TlVFWENYdUg0eHRia2d1Q01ZYUcwdno1WHJCa0JMeHRIVjNydlpBcmhPWXEyUl9uZ0thamhJOGNmQmtIQVBISU5FcGtPUFZHdTZ3ejN3azN5a0Y5TEVpZVYzSGstN1RCQmNFZnNoOW5ORlM2RnJ5Y2cw?oc=5", "summary": "<a href=\\"https://news.google.com/rss/articles/CBMixwFBVV95cUxOVjc3TzZxRWhJYTBpc1hfeTc1V3Z4YXZqeTlGRmZVd1hrN21YQUk4Vkg5a183cGlOaGNBcDdKTElBdHhKTU9OVXE0c01MNjFUeFk2VWJieDk2TlVFWENYdUg0eHRia2d1Q01ZYUcwdno1WHJCa0JMeHRIVjN3cmhPWXEyUl9uZ0thamhJOGNmQmtIQVBISU5FcGtPUFZHdTZ3ejN3azN5a0Y5TEVpZVYzSGstN1RCQmNFZnNoOW5ORlM2RnJ5Y2cw?oc=5\\" target=\\"_blank\\">Karnataka: Anganwadi workers to protest face recognition system, election duties on July 26</a>&nbsp;&nbsp;<font color=\\"#6f6f6f\\">Udayavani</font>", "published_date": "Thu, 24 Jul 2025 13:34:00 GMT", "updated_date": "Thu, 24 Jul 2025 13:34:00 GMT", "authors": [], "raw_entry": {"title": "Karnataka: Anganwadi workers to protest face recognition system, election duties on July 26 - Udayavani", "title_detail": {"type": "text/plain", "language": null, "base": "https://news.google.com/rss/search?q=Bengaluru+protest&hl=en-IN&gl=IN&ceid=IN:en", "value": "Karnataka: Anganwadi workers to protest face recognition system, election duties on July 26 - Udayavani"}, "link": "https://news.google.com/rss/articles/CBMixwFBVV95cUxOVjc3TzZxRWhJYTBpc1hfeTc1V3Z4YXZqeTlGRmZVd1hrN21YQUk4Vkg5a183cGlOaGNBcDdKTElBdHhKTU9OVXE0c01MNjFUeFk2VWJieDk2TlVFWENYdUg0eHRia2d1Q01ZYUcwdno1WHJCa0JMeHRIVjN3cmhPWXEyUl9uZ0thamhJOGNmQmtIQVBISU5FcGtPUFZHdTZ3ejN3azN5a0Y5TEVpZVYzSGstN1RCQmNFZnNoOW5ORlM2RnJ5Y2cw?oc=5", "id": "CBMixwFBVV95cUxOVjc3TzZxRWhJYTBpc1hfeTc1V3Z4YXZqeTlGRmZVd1hrN21YQUk4Vkg5a183cGlOaGNBcDdKTElBdHhKTU9OVXE0c01MNjFUeFk2VWJieDk2TlVFWENYdUg0eHRia2d1Q01ZYUcwdno1WHJCa0JMeHRIVjN3cmhPWXEyUl9uZ0thamhJOGNmQmtIQVBISU5FcGtPUFZHdTZ3ejN3azN5a0Y5TEVpZVYzSGstN1RCQmNFZnNoOW5ORlM2RnJ5Y2cw", "published": "Thu, 24 Jul 2025 13:34:00 GMT", "published_parsed": [2025, 7, 24, 13, 34, 0, 3, 205, 0], "summary": "<a href=\\"https://news.google.com/rss/articles/CBMixwFBVV95cUxOVjc3TzZxRWhJYTBpc1hfeTc1V3Z4YXZqeTlGRmZVd1hrN21YQUk4Vkg5a183cGlOaGNBcDdKTElBdHhKTU9OVXE0c01MNjFUeFk2VWJieDk2TlVFWENYdUg0eHRia2d1Q01ZYUcwdno1WHJCa0JMeHRIVjN3cmhPWXEyUl9uZ0thamhJOGNmQmtIQVBISU5FcGtPUFZHdTZ3ejN3azN5a0Y5TEVpZVYzSGstN1RCQmNFZnNoOW5ORlM2RnJ5Y2cw?oc=5\\" target=\\"_blank\\">Karnataka: Anganwadi workers to protest face recognition system, election duties on July 26</a>&nbsp;&nbsp;<font color=\\"#6f6f6f\\">Udayavani</font>", "summary_detail": {"type": "text/html", "language": null, "base": "https://news.google.com/rss/search?q=Bengaluru+protest&hl=en-IN&gl=IN&ceid=IN:en", "value": "<a href=\\"https://news.google.com/rss/articles/CBMixwFBVV95cUxOVjc3TzZxRWhJYTBpc1hfeTc1V3Z4YXZqeTlGRmZVd1hrN21YQUk4Vkg5a183cGlOaGNBcDdKTElBdHhKTU9OVXE0c01MNjFUeFk2VWJieDk2TlVFWENYdUg0eHRia2d1Q01ZYUcwdno1WHJCa0JMeHRIVjN3cmhPWXEyUl9uZ0thamhJOGNmQmtIQVBISU5FcGtPUFZHdTZ3ejN3azN5a0Y5TEVpZVYzSGstN1RCQmNFZnNoOW5ORlM2RnJ5Y2cw?oc=5\\" target=\\"_blank\\">Karnataka: Anganwadi workers to protest face recognition system, election duties on July 26</a>&nbsp;&nbsp;<font color=\\"#6f6f6f\\">Udayavani</font>"}, "source": {"href": "https://www.udayavani.com", "title": "Udayavani"}}}'
        return (pcoll.pipeline | 'CreateMockData' >> beam.Create([mock_payload_str]))


# Define a custom PTransform for extracting metadata
class ExtractNewsMetadataTransform(beam.PTransform):
    """
    A PTransform that extracts key metadata from parsed news article dictionaries.
    """
    def expand(self, pcoll):
        return (
                pcoll
                | 'ExtractFields' >> beam.Map(
            lambda article: {
                'article_id': article.get('article_id'),
                'title': article.get('title'),
                'published_date': article.get('published_date'),
                'source': article.get('source'),
                'link': article.get('link')
            }
        )
        )
