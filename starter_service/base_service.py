import threading
import time
from abc import abstractmethod, ABC

from environs import Env

from test_bed_adapter import TestBedOptions, TestBedAdapter
from test_bed_adapter.kafka.consumer_manager import ConsumerManager
from test_bed_adapter.kafka.log_manager import LogManager
from test_bed_adapter.kafka.producer_manager import ProducerManager

_env = Env()

CONSUME_ENV = _env('CONSUME', None)
PRODUCE_ENV = _env('PRODUCE', None)
CLIENT_ID_ENV = _env('CLIENT_ID', None)

KAFKA_HOST = _env('KAFKA_HOST', '127.0.0.1:3501')
SCHEMA_REGISTRY = _env('SCHEMA_REGISTRY', 'http://localhost:3502')
DEBUG = _env.bool("DEBUG", False)
PARTITIONER = _env("PARTITIONER", "random")
MESSAGE_MAX_BYTES = _env.int('MESSAGE_MAX_BYTES', 1000000)
HEARTBEAT_INTERVAL = _env.int('HEARTBEAT_INTERVAL', 10)
OFFSET_TYPE = _env('OFFSET_TYPE', 'earliest')


class StarterService(ABC):

    def __init__(self, CONSUME=None, PRODUCE=None, CLIENT_ID=None) -> None:
        super().__init__()
        self._validate_params(CONSUME, PRODUCE, CLIENT_ID)
        self._init_options()
        self._producers = {}
        self._consumers = {}
        self._test_bed_options = None

        self.logger.info(
            f"""Initializing\nCLIENT_ID {CONSUME_ENV}\n\tPRODUCE {PRODUCE_ENV}\n\tCONSUME {CLIENT_ID_ENV}\n\t""")

    def start(self):
        """Start the service"""
        self.logger.info("Initializing kafka")
        self._init_starter_params()

        def handle_message(message):
            self.logger.info("Received message")
            try:
                if DEBUG:
                    self.logger.info(f"Message {message}")
                self.handle_article(message)
            except Exception as e:
                self.logger.error(e)

        self._test_bed_adapter.initialize()
        listener_threads = []
        run = True

        # Create threads for each consume topic
        for topic in CONSUME_ENV.split(','):
            listener_threads.append(threading.Thread(
                target=ConsumerManager(
                    options=self._test_bed_options,
                    kafka_topic=topic,
                    handle_message=handle_message,
                    run=run
                ).listen)
            )

        # start all threads
        for thread in listener_threads:
            thread.daemon = True
            thread.start()

        # make sure we keep running until keyboardinterrupt

        while run:
            # make sure we check thread health every 10 sec
            time.sleep(10)
            for thread in listener_threads:
                if not thread.is_alive():
                    run = False
                    self.logger.error("Thread died, shutting down")

        # Stop test bed
        self._test_bed_adapter.stop()
        for producer in self._producers.values():
            producer.stop()

        # Clean after ourselves
        for thread in listener_threads:
            thread.join()

        raise Exception

    @abstractmethod
    def handle_article(self, article: dict):
        """Handle article"""
        pass

    def send_message(self, message, topics=None):
        """Send message to kafka topic"""
        if topics:
            for topic in topics.split(','):
                if topic in self._producers:
                    if DEBUG:
                        self.logger.info(f"Sending message to {topic}\n{message}")
                    self._producers[topic].send_messages(messages=[message])
        else:
            for topic, producer in self._producers.items():
                if DEBUG:
                    self.logger.info(f"Sending message to {topic}\n{message}")
                producer.send_messages(messages=[message])

    def _validate_params(self, CONSUME, PRODUCE, CLIENT_ID):
        """Validate that all required params are set"""
        global CONSUME_ENV, PRODUCE_ENV, CLIENT_ID_ENV
        CONSUME_ENV = CONSUME_ENV or CONSUME
        if CONSUME_ENV is None:
            raise ValueError("CONSUME cannot be None")
        self._CONSUME = CONSUME_ENV
        PRODUCE_ENV = PRODUCE_ENV or PRODUCE
        if PRODUCE_ENV is None:
            raise ValueError("PRODUCE cannot be None")
        self._PRODUCE = PRODUCE_ENV
        CLIENT_ID_ENV = CLIENT_ID_ENV or CLIENT_ID
        if CLIENT_ID_ENV is None:
            raise ValueError("CLIENT_ID cannot be None")
        self._CLIENT_ID = CLIENT_ID_ENV

    def _init_options(self):
        options = {
            "kafka_host": KAFKA_HOST,
            "schema_registry": SCHEMA_REGISTRY,
            "partitioner": PARTITIONER,
            "consumer_group": CLIENT_ID_ENV,
            "message_max_bytes": MESSAGE_MAX_BYTES,
            "offset_type": OFFSET_TYPE,
            "heartbeat_interval": HEARTBEAT_INTERVAL
        }
        self._test_bed_options = TestBedOptions(options)
        self._test_bed_adapter = TestBedAdapter(TestBedOptions(options))
        self.logger = LogManager(options=self._test_bed_options)

    def _init_starter_params(self):
        """Initialize all producers"""
        self._producers = {
            topic: ProducerManager(
                options=self._test_bed_options,
                kafka_topic=topic
            ) for topic in PRODUCE_ENV.split(',')
        }
