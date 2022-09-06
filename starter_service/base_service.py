import logging
import threading
import time
from abc import abstractmethod, ABC

from environs import Env

from test_bed_adapter import ProducerManager, TestBedOptions, TestBedAdapter, ConsumerManager

_env = Env()

KAFKA_HOST = _env('KAFKA_HOST', '127.0.0.1:3501')
SCHEMA_REGISTRY = _env('SCHEMA_REGISTRY', 'http://localhost:3502')
CONSUME = _env('CONSUME')
PRODUCE = _env('PRODUCE')
CLIENT_ID = _env('CLIENT_ID')
DEBUG = _env.bool("DEBUG", False)
PARTITIONER = _env("PARTITIONER", "random")
MESSAGE_MAX_BYTES = _env.int('MESSAGE_MAX_BYTES', 1000000)
HEARTBEAT_INTERVAL = _env.int('HEARTBEAT_INTERVAL', 10)
OFFSET_TYPE = _env('OFFSET_TYPE', 'earliest')
VACUUM_TIME = _env.int('VACUUM_TIME', 86400)

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')


class StarterService(ABC):
    logger = logging.getLogger()

    def __init__(self) -> None:
        super().__init__()
        self._test_bed_adapter = None

        self._producers = {}
        self._consumers = {}
        self._test_bed_options = None

        self._validate_params()
        self._init_starter_params()
        self.logger.info(
            f"Initializing\n\tCLIENT_ID {CLIENT_ID}\n\tPRODUCE {PRODUCE}\n\tCONSUME {CONSUME}")

    def start(self):
        """Start the service"""
        self.logger.info("Initializing kafka")
        options = {
            "kafka_host": KAFKA_HOST,
            "schema_registry": SCHEMA_REGISTRY,
            "partitioner": PARTITIONER,
            "consumer_group": CLIENT_ID,
            "message_max_bytes": MESSAGE_MAX_BYTES,
            "offset_type": OFFSET_TYPE,
            "heartbeat_interval": HEARTBEAT_INTERVAL
        }
        self._test_bed_options = TestBedOptions(options)
        self._test_bed_adapter = TestBedAdapter(TestBedOptions(options))

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

        # Create threads for each consume topic
        for topic in CONSUME.split(','):
            listener_threads.append(threading.Thread(
                target=ConsumerManager(
                    options=self._test_bed_options,
                    kafka_topic=topic,
                    handle_message=handle_message
                ).listen)
            )

        # start all threads
        for thread in listener_threads:
            thread.daemon = True
            thread.start()

        # make sure we keep running until keyboardinterrupt
        restart_timer = 0
        run = True

        while run:
            # make sure we check thread health every 10 sec
            time.sleep(10)
            for thread in listener_threads:
                if not thread.is_alive() or restart_timer >= VACUUM_TIME:
                    run = False
            else:
                restart_timer += 10

            # Stop test bed
        self._test_bed_adapter.stop()
        for producer in self._producers.values():
            producer.stop()

        # Clean after ourselves
        for thread in listener_threads:
            thread.join(5)

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
            for topic, producer in self._producers.values():
                if DEBUG:
                    self.logger.info(f"Sending message to {topic}\n{message}")
                producer.send_messages(messages=[message])

    def _validate_params(self):
        """Validate that all required params are set"""
        if CONSUME is None:
            raise ValueError("CONSUME cannot be None")
        if PRODUCE is None:
            raise ValueError("PRODUCE cannot be None")
        if CLIENT_ID is None:
            raise ValueError("CLIENT_ID cannot be None")

    def _init_starter_params(self):
        """Initialize all producers"""
        self._producers = {
            topic: ProducerManager(
                options=self._test_bed_options,
                kafka_topic=topic
            ) for topic in PRODUCE.split(',')
        }
