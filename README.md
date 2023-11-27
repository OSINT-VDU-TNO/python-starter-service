# python-starter-service

This is a starter service project template.
Use this template to create a new service project.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt # Or instead of `pip`, use `pip3`
```

## Getting started

First you need to provide ENV variables for the service to run.

### ENV variables

Main ENV variables:

- `CLIENT_ID` - client id of the service
- `REST_API_ENABLED` - enable/disable REST API (default: `true`)

## Kafka

- `CONSUME` - comma separated list of topics to consume
- `PRODUCE` - comma separated list of topics to produce
- `KAFKA_HOST` - Kafka host
- `SCHEMA_REGISTRY` - schema registry host
- `MAX_POLL_INTERVAL_MS` - max poll interval in ms (default: `600000`)
- `SESSION_TIMEOUT_MS` - session timeout in ms (default: `600000`)

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
    


## Uploading to PyPi

1. Ensure you have the necessary tools installed: Make sure you have `setuptools` and `wheel` installed. You can install them using `pip`:

```bash
# Build the distribution files: In the root directory of your project, run the following command to build the distribution files (wheel and source distribution):
pip install setuptools wheel twine
```

2. Build the distribution files: In the root directory of your project, run the following command to build the distribution files (wheel and source distribution):

```bash
# This command will generate the distribution files inside the dist directory.
# Remember to FIRST update the version number in your `setup.py` file for each new release to avoid conflicts.
python setup.py sdist bdist_wheel
```
This command will generate the distribution files inside the dist directory.

3. Register an account on PyPI: If you haven't done so already, create an account on PyPI and verify your email address.

4. Upload the package to PyPI: Use twine to upload the distribution files to PyPI:

```bash
# This command will prompt you to enter your PyPI username and password. Once provided, twine will upload the distribution files to PyPI.
twine upload dist/*
```
This command will prompt you to enter your PyPI username and password. Once provided, twine will upload the distribution files to PyPI.

5. Verify the package on PyPI: Visit your package page on [PyPI](https://pypi.org/project/osint-python-test-bed-adapter/) to ensure that the package has been successfully uploaded and published.

