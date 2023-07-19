import logging
from time import sleep

from starter_service.api import API
from starter_service.env import ENV
from starter_service.schemas import SchemaRegistry
from starter_service.sub_process import SubProcess
from test_bed_adapter import TestBedAdapter
from test_bed_adapter import TestBedOptions
from test_bed_adapter.kafka.consumer_manager import ConsumerManager
from test_bed_adapter.kafka.log_manager import LogManager
from test_bed_adapter.kafka.producer_manager import ProducerManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')


class KafkaAdapter(SubProcess):
    """Kafka adapter"""

    def __init__(self) -> None:
        super().__init__()
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
        # Initialize logger
        self.logger = logging.getLogger(__name__)
        self.error_msg = None

    def run(self):
        """Start the service"""
        try:
            self._test_bed_adapter = TestBedAdapter(self._test_bed_options)
            self.logger = LogManager(options=self._test_bed_options)
            self.logger.info(f"Kafka ClientId[{ENV.CLIENT_ID}], Consume[{ENV.CONSUME}], Produce[{ENV.PRODUCE}]")
            self.connect()
            self.error_msg = None
        except Exception as e:
            self.error_msg = f"Error starting service: {e}"
            self.logger.error(f"Error starting service: {e}, Restarting... in 30 seconds")
            sleep(30)
            self.run()

    def connect(self):
        """Start the service"""
        self._init_producers()
        self._test_bed_adapter.initialize()
        self._init_logger()
        # Create threads for each consume topic
        for topic in ENV.CONSUME.split(','):
            topic = topic.strip()
            if not topic:
                self.logger.warning("Empty topic, skipping")
                continue
            try:
                _consumer = ConsumerManager(
                    options=self._test_bed_options,
                    kafka_topic=topic,
                    handle_message=self._handle_message
                )
                self._consumers[topic] = _consumer
                self.logger.info(f"Registering schema from kafka for {topic}")
                SchemaRegistry.register_schema(_consumer.schema_str, topic)
            except:
                self.logger.error(f"Could not initialize consumer for topic {topic}")

        if self._callback:
            self._callback()

        # Start listening for messages
        for consumer in self._consumers.values():
            try:
                consumer.start()
                self.logger.info(f"Initializing listener for topic {topic}")
            except:
                self.logger.error("Could not start consumer")

        while self.running:
            for consumer in self._consumers.values():
                if not consumer.is_alive():
                    self.logger.error("Consumer thread died, exiting...")
                    self.running = False
                    break
            sleep(10)

        for consumer in self._consumers.values():
            try:
                consumer.stop()
                consumer.join()
            except:
                self.logger.error("Could not stop consumer")

        self.logger.info("Stopping service...")
        self.base_service.stop()

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
        for topic in ENV.PRODUCE.split(','):
            topic = topic.strip()
            if not topic:
                self.logger.warning("Empty topic, skipping")
                continue
            self.logger.info(f"Initializing producer for topic {topic}")
            _producer = ProducerManager(
                options=self._test_bed_options,
                kafka_topic=topic
            )
            self._producers[topic] = _producer
            SchemaRegistry.register_schema(_producer.schema_str, topic)

    def _init_logger(self):
        try:
            self.base_service.logger = self.logger
        except:
            pass

    def _init_options(self):
        _options = {
            "kafka_host": ENV.KAFKA_HOST,
            "schema_registry": ENV.SCHEMA_REGISTRY,
            "partitioner": ENV.PARTITIONER,
            "consumer_group": ENV.CLIENT_ID,
            "message_max_bytes": ENV.MESSAGE_MAX_BYTES,
            "offset_type": ENV.OFFSET_TYPE,
            "heartbeat_interval": ENV.HEARTBEAT_INTERVAL,
            "string_based_keys": ENV.STRING_BASED_KEYS,
            "ignore_timeout": ENV.IGNORE_TIMEOUT,
            "use_latest": ENV.USE_LATEST
        }
        self._test_bed_options = TestBedOptions(_options)

    def _handle_message(self, message, topic):
        self.logger.info(f"Received message for topic {topic}")
        if ENV.DEBUG:
            self.logger.info(f"Message {message}")

        funcs = API.get_func_by_consumer(topic)
        for consumer, producer, doc, func, _type in funcs:
            try:
                response = func(self._base_service, message)
                if producer and response:
                    self.logger.info(f"Sending response: {producer}, {str(response)[:100]}")
                    self.send_message(response, topics=producer)
            except Exception as e:
                self.logger.error(e)
