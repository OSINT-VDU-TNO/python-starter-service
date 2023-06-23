from starter_service.api import API
from starter_service.base_service import StarterService
from starter_service.env import ENV

ENV.SET("CONSUME", "article_raw_xx")
ENV.SET("PRODUCE", "article_raw_en")


class ManualKafka(StarterService):
    path = "app"

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
            testing=False  # Default is False, when deploying to production, set to False
        )

    @API.post(consumer=ENV.CONSUME, producer=ENV.PRODUCE, doc="Test")
    def handle_message(self, message: dict):
        message['language'] = 'en'
        return message

    def api_callback(self):
        """API callback"""

        @self.api.fast_api.post("/test_async_endpoint")
        async def async_post_endpoint(data: dict):
            data['language'] = 'en'
            return data

        @self.api.fast_api.post("/test_endpoint")
        def test_post_endpoint(data: dict) -> dict:
            data['language'] = 'en'
            return data

        def post_test(data: dict) -> dict:
            data['language'] = 'en'
            return data

        self.api.fast_api.add_api_route('/test_manual', post_test, methods=['POST'])


if __name__ == '__main__':
    ManualKafka().start()
