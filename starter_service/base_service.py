import logging
import os
import threading
import time
from abc import abstractmethod, ABC
from distutils.util import strtobool

from test_bed_adapter import TestBedAdapter, TestBedOptions


class StarterService(ABC):

    def __init__(self, CONSUME: str = None, PRODUCE: str = None, CLIENT_ID: str = None) -> None:
        super().__init__()
        self._test_bed_adapter = None

        self._KAFKA_HOST = os.environ.get('KAFKA_HOST', '127.0.0.1:3501')
        self._SCHEMA_REGISTRY = os.environ.get('SCHEMA_REGISTRY', 'http://localhost:3502')
        self._CONSUME = os.environ.get('CONSUME', CONSUME)
        self._PRODUCE = os.environ.get('PRODUCE', PRODUCE)
        self._CLIENT_ID = os.environ.get('CLIENT_ID', CLIENT_ID)
        self._DEBUG = strtobool(os.getenv("DEBUG", "false"))

        self._validate_params()
        self._init_starter_params()
        self._init_logger()
        self.logger.info(
            f"Initializing\n\tCLIENT_ID {self._CLIENT_ID}\n\tPRODUCE {self._PRODUCE}\n\tCONSUME {self._CONSUME}")

    def start(self):
        self.logger.info("initializing kafka")
        options = {
            "kafka_host": self._KAFKA_HOST,
            "schema_registry": self._SCHEMA_REGISTRY,
            "fetch_all_versions": False,
            "from_off_set": True,
            "client_id": self._CLIENT_ID,
            "consume": self._CONSUME,
            "produce": self._PRODUCE
        }

        self._test_bed_adapter = TestBedAdapter(TestBedOptions(options))

        def handle_message(message):
            self.logger.info("Received message")
            try:
                article = message['decoded_value'][0]
                if self._DEBUG:
                    self.logger.info(f"Article {article}")
                self.handle_article(article)
            except Exception as e:
                self.logger.error(e)

        self._test_bed_adapter.initialize()
        listener_threads = []

        # Create threads for each consume topic
        for topic in self._CONSUME:
            self._test_bed_adapter.consumer_managers[topic].on_message += handle_message

            listener_threads.append(threading.Thread(
                target=self._test_bed_adapter.consumer_managers[topic].listen_messages))

        # start all threads
        for thread in listener_threads:
            thread.start()

        # make sure we keep running until keyboardinterrupt
        try:
            while True:
                time.sleep(1)
                # Let the reception threads run
        except KeyboardInterrupt:
            self.logger.debug('Interrupted!')

        # Stop test bed
        self._test_bed_adapter.stop()

        # Clean after ourselves
        for thread in listener_threads:
            thread.join()

    @abstractmethod
    def handle_article(self, article: dict):
        pass

    def send_message(self, message):
        for topic in self._PRODUCE:
            if self._DEBUG:
                self.logger.info(f"Sending message to {topic}\n{message}")
            self._test_bed_adapter.producer_managers[topic].send_messages([{"message": message}])

    def _validate_params(self):
        if self._CONSUME is None:
            raise ValueError("CONSUME cannot be None")
        if self._PRODUCE is None:
            raise ValueError("PRODUCE cannot be None")
        if self._CLIENT_ID is None:
            raise ValueError("CLIENT_ID cannot be None")

    def _init_logger(self):
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def _init_starter_params(self):
        self._CONSUME = [param.strip() for param in self._CONSUME.split(',')]
        self._PRODUCE = [param.strip() for param in self._PRODUCE.split(',')]