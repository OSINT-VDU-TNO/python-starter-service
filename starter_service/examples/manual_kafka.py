import os

os.environ['CONSUME'] = 'article_raw_xx'
os.environ['PRODUCE'] = 'article_raw_en,article_raw_lt,metadata_item_update'

from starter_service.base_service import StarterService
from starter_service.api import API


class ManualKafka(StarterService):
    name = "manual_kafka"
    path = "app"

    def __init__(self):
        super().__init__()

    def health(self):
        return

    def ready(self):
        return True

    def kafka_callback(self):
        """Kafka callback"""
        self.logger.info("Kafka callback")
        # When testing is True, the message is not sent to Kafka, but is printed to the console
        self.send_message({
            "articleId": "string",
            "origin": "string",
            "data": []
        }, 'metadata_item_update',
            testing=True  # Default is False, when deploying to production, set to False
        )

    @API.post(consumer="article_raw_xx", doc="Process article_raw_xx and send to article_raw_en, article_raw_lt")
    def handle_message(self, message: dict):
        message['language'] = 'en'
        self.send_message(message, 'article_raw_en')

        message['language'] = 'lt'
        self.send_message(message, 'article_raw_lt')
        return {}


if __name__ == '__main__':
    ManualKafka()
