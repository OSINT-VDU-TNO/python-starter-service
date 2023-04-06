import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

os.environ['CLIENT_ID'] = 'manual_kafka'
os.environ['CONSUME'] = 'article_raw_xx'
os.environ['PRODUCE'] = 'article_raw_en,article_raw_lt'
os.environ['REST_API_ENABLED'] = 'True'

from base_service import StarterService
from api import API


class ManualKafka(StarterService):

    def __init__(self):
        super().__init__()

    def health(self):
        return

    def ready(self):
        return True

    @API.post(consumer="article_raw_xx", doc="Process article_raw_xx and send to article_raw_en, article_raw_lt")
    def handle_message(self, message: dict):
        message['language'] = 'en'
        self.send_message(message, 'article_raw_en')
        message['language'] = 'lt'
        self.send_message(message, 'article_raw_lt')
        return {}


if __name__ == '__main__':
    ManualKafka()
