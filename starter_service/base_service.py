import logging
import traceback
from abc import ABC, abstractmethod

from api_server import APIServer
from kafka_adapter import KafkaAdapter

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')


class StarterService(ABC):

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._kafka = None
        self._api = None
        self._schemas = None
        self._functions = None
        self._initialize()

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

        def _schema_callback():
            """Callback for Schema Registry, called when Schema Registry is ready or when an error occurs"""
            self._logger.info("Schema callback")
            self._register_api(_kafka_status)

        def _kafka_callback():
            """Callback for Kafka Adapter, called when Kafka Adapter is ready or when an error occurs"""
            self._logger.info("Kafka callback")
            self._register_schemas(_schema_callback)

        """Initialize services"""
        _kafka_status = "ok"
        self._logger.info("Initializing kafka")
        try:
            self._kafka = KafkaAdapter(callback=_kafka_callback, base_service=self)
            self._kafka.start()
            self._logger.info(f"Kafka initialized.")
        except Exception as e:
            _kafka_status = f"Error: {e}"
            _kafka_callback()
            self._logger.error(f'Error initializing kafka: {e}')

    def _register_schemas(self, callback):
        try:
            callback()
        except Exception as e:
            self._logger.error(f'Error initializing schema registry: {e}')

    def _register_api(self, _kafka_status):
        try:
            self._api = APIServer(ready=self.ready, health=self.health, kafka_status=_kafka_status,
                                  base_service=self)
            self._api.start()
            self._logger.info(f"REST API initialized.")
        except Exception as e:
            self._logger.error(f'Error initializing api: {e}')
            traceback.print_exc()

        if self._kafka is None and self._api is None:
            self._logger.error('No services initialized. Shutting down.')
            exit(1)

    def send_message(self, message, topic):
        """Send message to Kafka"""
        if self._kafka is None:
            raise Exception("Kafka is not initialized")
        self._kafka.send_message(message, topics=topic, testing=True)
