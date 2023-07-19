import logging
import sys
import threading
from abc import ABC, abstractmethod
from time import sleep

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
        self.running = True

        # Initialize services
        self.kafka = None
        self.api = None

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
        self.name = ENV.CLIENT_ID = ENV.CLIENT_ID or self.name or self.__class__.__name__
        # Initialize schema registry
        SchemaRegistry.initialize(self.path)
        # Initialize services
        self._init_kafka()
        # Initialize API
        self._init_api()

    def _init_api(self):
        try:
            self.api = APIServer(name=self.name, ready=self.ready, health=self.health)
            self.api.callback = self.api_callback
            self.api.base_service = self
            self.logger.info(f"API initialized.")
        except Exception as e:
            self.logger.error(f'Error initializing API: {e}')
            self.api_callback(error=e)

    def _init_kafka(self):
        try:
            self.kafka = KafkaAdapter()
            self.kafka.callback = self.kafka_callback
            self.kafka.base_service = self
            self.logger.info(f"Kafka initialized.")
        except Exception as e:
            self.logger.error(f'Error initializing kafka: {e}')
            self.kafka_callback(error=e)

    def start(self):
        """Start the service"""
        try:
            # Start Kafka
            if self.kafka:
                self.logger.info("Starting service Kafka...")
                self.kafka.start()
            # Wait for kafka to start and register schemas
            self.logger.info("Waiting for Kafka to start and register schemas...")
            sleep(3)
            # Synchronous callback
            self.callback()
            # Asynchronous callback
            threading.Thread(target=self.async_callback, daemon=True).start()
            # Start API
            if self.api:
                self.logger.info("Starting service API...")
                self.api.run()
            # Check if services are initialized
            if self.kafka is None and self.api is None:
                raise Exception('No services initialized. Shutting down.')

        except Exception as e:
            self.logger.info(f"Error starting services: {e}")
            self.stop()

    def stop(self):
        """Stop the service"""
        self.logger.info("Stopping service...")
        try:
            self.logger.info("Stopping Kafka...")
            if self.kafka:
                self.kafka.stop()
                # self.kafka.join()
        except Exception as e:
            self.logger.error(f"Error stopping Kafka: {e}")
        try:
            self.logger.info("Stopping API...")
            if self.api:
                self.api.stop()
                # self.api.join()
        except Exception as e:
            self.logger.error(f"Error stopping API: {e}")
        sys.exit(0)

    def send_message(self, message, topic, testing=True):
        """Send message to Kafka"""
        if self.kafka is None:
            raise Exception("Kafka is not initialized")
        self.kafka.send_message(message, topics=topic, testing=testing)

    def is_ok(self) -> bool:
        """Return True if service is ready to receive messages, False otherwise."""
        return self.kafka is not None and self.api is not None

    def kafka_callback(self, **kwargs):
        """Override this method to callback after service is initialized"""
        pass

    def api_callback(self, **kwargs):
        """Override this method to callback after service is initialized"""
        pass

    def callback(self, **kwargs):
        """Override this method to callback after service is initialized"""
        pass

    def async_callback(self, **kwargs):
        """Override this method to callback after service is initialized"""
        pass
