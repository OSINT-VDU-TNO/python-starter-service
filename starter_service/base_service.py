import logging
import traceback
from abc import ABC, abstractmethod

from starter_service.api_server import APIServer
from starter_service.env import ENV
from starter_service.kafka_adapter import KafkaAdapter
from starter_service.schemas import SchemaRegistry

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')


class StarterService(ABC):
    """Base class for all services."""
    name = None  # Change this to the name of your service or use CLIENT_ID environment variable
    path = None  # Path for local schemas, use SCHEMA_PATH environment variable

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._kafka = None
        self._api = None
        self._schemas = None
        self._functions = None
        self._initialize()
        self._schema_registry = None

    @abstractmethod
    def ready(self) -> bool:
        """Return True if service is ready to receive messages, False otherwise."""
        pass

    @abstractmethod
    def health(self) -> str:
        """Return service health status."""
        pass

    def _initialize(self):
        """Initialize services"""
        self.name = ENV.CLIENT_ID = ENV.CLIENT_ID or self.name or self.__class__.__name__

        """Initialize Schema Registry"""
        SchemaRegistry.initialize(self.path)

        def _api_callback():
            """Callback for API Server, called when API Server is ready or when an error occurs"""
            self.logger.info("API callback")
            self.api_callback()

        def _kafka_callback():
            """Callback for Kafka Adapter, called when Kafka Adapter is ready or when an error occurs"""
            self.logger.info("Kafka callback")
            self.kafka_callback()
            self._register_api(_kafka_status, _api_callback)

        """Initialize services"""
        _kafka_status = "ok"
        self.logger.info("Initializing kafka")
        try:
            self._kafka = KafkaAdapter(callback=_kafka_callback, base_service=self)
            self._kafka.start()
            self.logger.info(f"Kafka initialized.")
        except Exception as e:
            _kafka_status = f"Error: {e}"
            _kafka_callback()
            self.logger.error(f'Error initializing kafka: {e}')

    def _register_api(self, _kafka_status, callback):
        try:
            self._api = APIServer(name=self.name, ready=self.ready, health=self.health, kafka_status=_kafka_status,
                                  base_service=self, callback=callback)
            self._api.start()
            self.logger.info(f"REST API initialized.")
        except Exception as e:
            self.logger.error(f'Error initializing api: {e}')
            traceback.print_exc()

        if self._kafka is None and self._api is None:
            self.logger.error('No services initialized. Shutting down.')
            exit(1)

    def send_message(self, message, topic, testing=True):
        """Send message to Kafka"""
        if self._kafka is None:
            raise Exception("Kafka is not initialized")
        self._kafka.send_message(message, topics=topic, testing=testing)

    def kafka_callback(self):
        """Override this method to callback after service is initialized"""
        pass

    def api_callback(self):
        """Override this method to callback after service is initialized"""
        pass
