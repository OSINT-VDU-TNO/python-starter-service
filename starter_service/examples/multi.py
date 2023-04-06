import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

os.environ['CLIENT_ID'] = 'multi'
os.environ['CONSUME'] = 'article_raw_xx'
os.environ['PRODUCE'] = 'article_raw_en,article_raw_lt,article_raw_nl'
os.environ['REST_API_ENABLED'] = 'True'

from base_service import StarterService
from api import API


class MultiRoutes(StarterService):

    def __init__(self):
        super().__init__()

    def health(self):
        return

    def ready(self):
        return True

    @API.post(consumer="article_raw_xx", producer="article_raw_en",
              doc="Process article_raw_xx and posts article_raw_en")
    def handle_message_en(self, message: dict):
        message['language'] = 'en'
        return message

    @API.post(consumer="article_raw_xx", producer="article_raw_lt",
              doc="Process article_raw_xx and posts article_raw_en")
    def handle_message_lt(self, message: dict):
        message['language'] = 'lt'
        return message

    @API.post(consumer="article_raw_xx", producer="article_raw_nl",
              doc="Process article_raw_xx and posts article_raw_nl")
    def handle_message_nl(self, message: dict):
        message['language'] = 'nl'
        return message


if __name__ == '__main__':
    MultiRoutes()
