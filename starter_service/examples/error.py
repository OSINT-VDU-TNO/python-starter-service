import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

os.environ['CONSUME'] = 'article_raw_en'
os.environ['PRODUCE'] = 'metadata_item_key_en'
os.environ['REST_API_ENABLED'] = 'True'

from starter_service.base_service import StarterService
from starter_service.api import API


class SingleRoute(StarterService):
    name = "single_error"

    def health(self):
        return "OK"

    def ready(self):
        return True

    @API.post(consumer="article_raw_en", producer="metadata_item_key_en", doc="Process raw article and return metadata")
    def handle_message(self, message: dict):
        raise Exception("Bad request")


if __name__ == '__main__':
    SingleRoute()
