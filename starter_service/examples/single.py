import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

os.environ['CLIENT_ID'] = 'single'
os.environ['CONSUME'] = 'article_raw_en'
os.environ['PRODUCE'] = 'metadata_item_key_en'
os.environ['REST_API_ENABLED'] = 'True'

from base_service import StarterService
from api import API


class SingleRoute(StarterService):

    def health(self):
        return "OK"

    def ready(self):
        return True

    @API.post(consumer="article_raw_en", producer="metadata_item_key_en", doc="Process raw article and return metadata")
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
    SingleRoute()
