from starter_service.api import API
from starter_service.base_service import StarterService
from starter_service.env import ENV

ENV.SET("CONSUME", "article_raw_en")
ENV.SET("PRODUCE", "metadata_item_key_en")


class SingleRoute(StarterService):
    name = "single_error"

    def health(self):
        return "OK"

    def ready(self):
        return True

    @API.post(consumer=ENV.CONSUME, producer=ENV.PRODUCE, doc="Process raw article and return metadata")
    def handle_message(self, message: dict):
        raise Exception("Bad request")


if __name__ == '__main__':
    SingleRoute().start()
