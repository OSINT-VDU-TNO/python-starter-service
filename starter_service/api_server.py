import datetime
import logging

import uvicorn
from fastapi import FastAPI, APIRouter
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError
from starlette.responses import Response, JSONResponse
from starlette.status import HTTP_200_OK
from starter_service.api import API
from starter_service.env import ENV
from starter_service.sub_process import SubProcess
from uvicorn import Config


class APIServer(SubProcess):

    def __init__(self, name=None, ready: callable = None, health: callable = None, **kwargs):
        super().__init__()
        self.name = name
        self.server = None
        self.logger = logging.getLogger(__name__)

        self._validated()
        self._fast_api = FastAPI(title=self.name)
        self._router = APIRouter()

        @self._fast_api.exception_handler(Exception)
        async def validation_exception_handler(request, err):
            base_error_message = f"Failed to execute: {request.method}: {request.url}"
            # Change here to LOGGER
            return JSONResponse(status_code=400, content={"message": f"{base_error_message}. Detail: {err}"})

        @self._fast_api.exception_handler(ValidationError)
        async def validation_exception_handler(request, err):
            base_error_message = f"Failed to execute: {request.method}: {request.url}"
            # Change here to LOGGER
            return JSONResponse(status_code=400, content={"message": f"{base_error_message}. Detail: {err}"})

        # service
        self._ready = ready
        self._health = health

        self._uptime = None

    @property
    def fast_api(self):
        return self._fast_api

    @property
    def router(self):
        return self._router

    def _register_static_routes(self):
        self.logger.info("Registering static routes")

        @self._router.get("/")
        def root():
            from starter_service.schemas import SchemaRegistry
            return {
                "client_id": ENV.CLIENT_ID,
                "uptime": self._uptime,
                "docs": "/docs",
                "redoc": "/redoc",
                "openapi": "/openapi.json",
                "kafka": {
                    "error": self.base_service.kafka_error} if self.base_service and self.base_service.kafka_error else {
                    "status": "ok",
                    "host": ENV.KAFKA_HOST,
                    "schema_registry": ENV.SCHEMA_REGISTRY,
                    "partitioner": ENV.PARTITIONER,
                    "message_max_bytes": ENV.MESSAGE_MAX_BYTES,
                    "heartbeat_interval": ENV.HEARTBEAT_INTERVAL,
                    "offset_type": ENV.OFFSET_TYPE,
                    "ignore_timeout": ENV.IGNORE_TIMEOUT,
                    "use_latest": ENV.USE_LATEST,
                    "topics": {
                        "consume": ENV.CONSUME,
                        "produce": ENV.PRODUCE
                    }
                },
                "environment": {key: value for key, value in ENV.__dict__.items() if
                                not key.startswith("_") and key not in ["SET", "GET", "update"]},
                "schemas:": SchemaRegistry.get_schemas_dict(),
                "methods": API.functions
            }

        @self._router.get("/api/health", tags=["status"])
        def health(verbose: bool = False):
            """Return health status"""
            health = self._health()
            if health:
                if verbose:
                    return health
                else:
                    return Response(status_code=HTTP_200_OK)
            else:
                return Response(status_code=503)

        @self._router.get("/api/ready", tags=["status"])
        def ready():
            """Return 200 OK if server is ready"""
            response = self._ready()
            if response:
                return Response(status_code=HTTP_200_OK)
            else:
                return Response(status_code=503)

    def run(self):
        """Start the server"""
        self.logger.info("Starting API server")

        self._register_static_routes()
        self._register_dynamic_routes()
        self._fast_api.include_router(self._router)

        self._running = True
        self._uptime = datetime.datetime.now().isoformat()
        if self.callback:
            self.callback()
        try:
            self.logger.info(f"Starting API server on {ENV.REST_API_HOST}:{ENV.REST_API_PORT}")
            server = uvicorn.Server(config=Config(self._fast_api, host=ENV.REST_API_HOST, port=ENV.REST_API_PORT))
            server.run()
        except Exception as e:
            self.logger.error(f"Failed to start API server: {e}")
            self.stop()

    def stop(self):
        self.logger.info("Stopping API server")
        self.running = False
        self.server.stop()

    def _validated(self):
        """Validate that the server is configured correctly"""
        if ENV.REST_API_ENABLED is False:
            raise Exception("REST API is disabled. To enable REST API set REST_API_ENABLED to true")

    def _register_dynamic_routes(self):
        """Register routes that are dynamically added by the user"""
        self.logger.info("Registering dynamic routes")
        for consumer, producer, doc, func, _type in API.functions:
            self._register_route(consumer, producer, doc, func, _type)

    def _register_route(self, consumer, producer, doc, func, _type):
        """Register a route"""
        from starter_service.schemas import SchemaRegistry
        consumer_class = SchemaRegistry.get_schema(consumer)
        producer_class = SchemaRegistry.get_schema(producer)

        self.logger.info(f"Registering route {consumer}:{consumer_class} -> {producer}:{producer_class} ({doc})")
        func_wrapper = lambda message: func(self.base_service, message) \
            if isinstance(message, str) else func(self.base_service, jsonable_encoder(message))
        path = f"/api{f'/{consumer}' if consumer else ''}{f'/{producer}' if producer else ''}"

        func_wrapper.__annotations__ = {'message': consumer_class, 'return': producer_class}
        self._router.add_api_route(path, func_wrapper, methods=[_type], response_model=producer_class, tags=["topics"],
                                   summary=doc)
