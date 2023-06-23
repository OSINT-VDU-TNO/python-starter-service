from starter_service.api import API
from starter_service.base_service import StarterService
from starter_service.env import ENV

ENV.SET("CONSUME", "article_raw_en")
ENV.SET("PRODUCE", "metadata_item_key_en")


class SingleRoute(StarterService):
    name = "single_route"
    path = "app"

    def health(self):
        return "OK"

    def ready(self):
        return True

    def kafka_callback(self):
        """Kafka callback"""
        self.logger.info("Kafka callback")

    def api_callback(self):
        """API callback"""
        self.logger.info("API callback")

    @API.post(consumer=ENV.CONSUME, producer=ENV.PRODUCE, doc="Process raw article and return metadata")
    def handle_message(self, message: dict):
        return {
            "articleId": message['id'],
            "origin": "string",
            "data": [
                {
                    "type": "string",
                    "value": "string",
                    "confidence": 0,
                    "metadata": {
                        "string": "string"
                    }
                }
            ]
        }


if __name__ == '__main__':
    SingleRoute().start()
