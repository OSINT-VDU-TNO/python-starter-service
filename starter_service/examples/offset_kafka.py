from starter_service.api import API
from starter_service.base_service import StarterService
from starter_service.env import ENV

ENV.SET("CONSUME", "article_raw_xx")
ENV.SET("PRODUCE", "article_raw_en,article_raw_lt,metadata_item_update")

ENV.SET("OFFSET_TYPE", 'earliest')
ENV.SET("IGNORE_TIMEOUT", 3)
ENV.SET("USE_LATEST", True)


class ManualKafka(StarterService):
    path = "app"

    def __init__(self):
        super().__init__()

    def health(self):
        return

    def ready(self):
        return True

    def kafka_callback(self):
        """Kafka callback"""
        # When testing is True, the message is not sent to Kafka, but is printed to the console
        self.send_message({
            "articleId": "string",
            "origin": "string",
            "data": []
        }, 'metadata_item_update',
            testing=False  # Default is False, when deploying to production, set to False
        )

    @API.post(consumer=ENV.CONSUME, doc="Process article_raw_xx and send to article_raw_en, article_raw_lt")
    def handle_message(self, message: dict):
        message['language'] = 'en'
        self.send_message(message, 'article_raw_en')

        message['language'] = 'lt'
        self.send_message(message, 'article_raw_lt')
        return {}


if __name__ == '__main__':
    ManualKafka().start()
