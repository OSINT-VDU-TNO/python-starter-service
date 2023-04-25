import logging
import threading
import time
import traceback
from threading import Thread

from test_bed_adapter import TestBedOptions, TestBedAdapter
from test_bed_adapter.kafka.consumer_manager import ConsumerManager
from test_bed_adapter.kafka.log_manager import LogManager
from test_bed_adapter.kafka.producer_manager import ProducerManager

from starter_service.api import API
from starter_service.env import ENV
from starter_service.schemas import SchemaRegistry

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')


class KafkaAdapter(Thread):
    """Kafka adapter"""

    def __init__(self, callback=None, base_service=None) -> None:
        super().__init__()
        self.daemon = True
        # Initialize logger
        self.logger = None
        # Initialize test bed adapter
        self._test_bed_adapter = None
        # Initialize test bed options
        self._test_bed_options = None

        # Validate environment variables
        self._validate_params()
        # Initialize test bed options
        self._init_options()
        # Initialize producers and consumers
        self._producers = {}
        self._consumers = {}

        self._callback = callback
        self._base_service = base_service

    def start(self):
        """Start the service"""
        self._init_producers()
        self._test_bed_adapter.initialize()
        listener_threads = []
        run = True

        # Create threads for each consume topic
        for topic in ENV.CONSUME:
            _consumer = ConsumerManager(
                options=self._test_bed_options,
                kafka_topic=topic,
                handle_message=self._handle_message,
                run=lambda: run
            )
            self.logger.info(f"Registering schema from kafka for {topic}")
            SchemaRegistry.register_schema(_consumer.schema_str, topic)
            self.logger.info(f"Initializing listener for topic {topic}")
            listener_threads.append(threading.Thread(
                target=_consumer.listen)
            )

        # start all threads
        for thread in listener_threads:
            thread.daemon = True
            thread.start()

        if self._callback:
            self._callback()

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
            self.logger.info(f"Stopping producer {producer}")
            producer.stop()

        # Clean after ourselves
        for thread in listener_threads:
            thread.join()

        raise Exception

    def send_message(self, message, topics=None, testing=False):
        """Send message to kafka topic"""
        if testing:
            self.logger.info(f"Sending test message to {topics}\n{message}")
            return

        if topics:
            if isinstance(topics, str):
                topics = [topic.strip() for topic in topics.split(',')]
            for topic in topics:
                self.logger.info(f"Sending message to {topic}")
                if topic in self._producers:
                    if ENV.DEBUG:
                        self.logger.info(f"Sending message to {topic}\n{message}")
                    self._producers[topic].send_messages(messages=[message])
        else:
            for topic, producer in self._producers.items():
                if ENV.DEBUG:
                    self.logger.info(f"Sending message to {topic}\n{message}")
                producer.send_messages(messages=[message])

    def _validate_params(self):
        """Validate that all required params are set"""
        if ENV.CLIENT_ID is None:
            raise ValueError("CLIENT_ID cannot be None. Please set it in the environment.")
        if not ENV.CONSUME and not ENV.PRODUCE:
            raise ValueError("Both CONSUME and PRODUCE environment parameters cannot be None.")

    def _init_producers(self):
        """Initialize all producers"""
        for topic in ENV.PRODUCE:
            self.logger.info(f"Initializing producer for topic {topic}")
            _producer = ProducerManager(
                options=self._test_bed_options,
                kafka_topic=topic
            )
            self._producers[topic] = _producer
            SchemaRegistry.register_schema(_producer.schema_str, topic)

    def _init_options(self):
        _options = {
            "kafka_host": ENV.KAFKA_HOST,
            "schema_registry": ENV.SCHEMA_REGISTRY,
            "partitioner": ENV.PARTITIONER,
            "consumer_group": ENV.CLIENT_ID,
            "message_max_bytes": ENV.MESSAGE_MAX_BYTES,
            "offset_type": ENV.OFFSET_TYPE,
            "heartbeat_interval": ENV.HEARTBEAT_INTERVAL,
            "string_based_keys": ENV.STRING_BASED_KEYS
        }
        self._test_bed_options = TestBedOptions(_options)
        self._test_bed_adapter = TestBedAdapter(self._test_bed_options)
        self.logger = LogManager(options=self._test_bed_options)
        self.logger.info(f"Initializing kafka: ClientId[{ENV.CLIENT_ID}],"
                         f" Consume{ENV.CONSUME}, Produce{ENV.PRODUCE}")
        self.logger.info(f"Connected to kafka: {_options}")

    def _handle_message(self, message, topic):
        self.logger.info(f"Received message for topic {topic}")
        if ENV.DEBUG:
            self.logger.info(f"Message {message}")

        funcs = API.get_func_by_consumer(topic)
        for consumer, producer, doc, func, _type in funcs:
            self.logger.info(f'Found function form {consumer}, to {producer}')
            try:
                response = func(self._base_service, message)
                if producer and response:
                    self.logger.info(f"Sending response: {producer}, {str(response)[:100]}")
                    self.send_message(response, topics=producer)
            except Exception as e:
                traceback.print_exc()
                self.logger.error(e)
