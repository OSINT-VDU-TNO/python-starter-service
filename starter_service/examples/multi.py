from starter_service.api import API
from starter_service.base_service import StarterService
from starter_service.env import ENV

ENV.SET("CONSUME", "article_raw_xx")
ENV.SET("PRODUCE", "article_raw_en,article_raw_lt,article_raw_nl")


class MultiRoutes(StarterService):
    name = "multi"
    producers = ENV.PRODUCE.split(',')

    def __init__(self):
        super().__init__()

    def health(self):
        return

    def ready(self):
        return True

    @API.post(consumer=ENV.CONSUME, producer=producers[0],
              doc="Process article_raw_xx and posts article_raw_en")
    def handle_message_en(self, message: dict):
        message['language'] = 'en'
        return message

    @API.post(consumer=ENV.CONSUME, producer=producers[1],
              doc="Process article_raw_xx and posts article_raw_lt")
    def handle_message_lt(self, message: dict):
        message['language'] = 'lt'
        return message

    @API.post(consumer=ENV.CONSUME, producer=producers[2],
              doc="Process article_raw_xx and posts article_raw_nl")
    def handle_message_nl(self, message: dict):
        message['language'] = 'nl'
        return message


if __name__ == '__main__':
    MultiRoutes().start()
