# python-starter-service

This is a starter service project template.
Use this template to create a new service project.

## Getting started

First you need to provide ENV variables for the service to run.

### ENV variables

Main ENV variables:

- `CLIENT_ID` - client id of the service
- `REST_API_ENABLED` - enable/disable REST API (default: `true`)

## Kafka

- `CONSUME` - comma separated list of topics to consume
- `PRODUCE` - comma separated list of topics to produce
- `KAFKA_HOST` - kafka host
- `SCHEMA_REGISTRY` - schema registry host

## Usage

Check the provided examples in the `examples` folder.

## Example

    from starter_service import StarterService
    
    class ExampleService(StarterService):

        def health(self):
            """ Health check endpoint """
            return "OK"

        def ready(self):
            """ Ready check endpoint """    
            return True
    
        def process(self, message):
            """ Process message """

        @API.post(consumer="article_raw_en", producer="metadata_item_key_en", doc="Process raw article and return metadata")
        def handle_message(self, message: dict):
            """ Process raw article and return metadata """
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
            ExampleService()
    


