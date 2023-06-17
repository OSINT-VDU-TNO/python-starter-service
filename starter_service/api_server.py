import datetime
import logging
import threading

from fastapi import FastAPI, APIRouter
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError
from starlette.responses import Response, JSONResponse
from starlette.status import HTTP_200_OK

from starter_service.api import API
from starter_service.env import ENV
from starter_service.schemas import SchemaRegistry


class APIServer:

    def __init__(self, name=None, ready: callable = None, health: callable = None, kafka_status: str = None,
                 base_service=None, callback=None, **kwargs):
        self.name = name
        self._validated()
        self._fast_api = FastAPI(title=self.name)
        self._router = APIRouter()
        self.callback = callback

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

        self.host = ENV.REST_API_HOST
        self.port = ENV.REST_API_PORT
        self._logger = logging.getLogger(__name__)

        self._ready = ready
        self._health = health
        self._kafka_status = kafka_status

        self._thread = None
        self._uptime = None
        self._running = False
        self._base_service = base_service

        self._register_static_routes()
        self._register_dynamic_routes()
        self._fast_api.include_router(self._router)

    @property
    def fast_api(self):
        return self._fast_api

    @property
    def router(self):
        return self._router

    def _register_static_routes(self):
        self._logger.info("Registering static routes")

        @self._router.get("/")
        def root():
            return {
                "client_id": ENV.CLIENT_ID,
                "uptime": self._uptime,
                "docs": "/docs",
                "redoc": "/redoc",
                "openapi": "/openapi.json",
                "kafka": {"error": self._kafka_status} if self._kafka_status != "ok" else {
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

    def start(self):
        """Start the server"""
        self._logger.info("Starting API server")
        self._running = True
        self._uptime = datetime.datetime.now().isoformat()
        self._thread = threading.Thread(target=self._run)
        self._thread.start()
        self.callback()

    def stop(self):
        self._logger.info("Stopping API server")
        self._running = False
        self._thread.join()

    def _run(self):
        import uvicorn
        uvicorn.run(self._fast_api, host=ENV.REST_API_HOST, port=ENV.REST_API_PORT)

    def _validated(self):
        """Validate that the server is configured correctly"""
        if ENV.REST_API_ENABLED is False:
            raise Exception("REST API is disabled. To enable REST API set REST_API_ENABLED to true")

    def _register_dynamic_routes(self):
        """Register routes that are dynamically added by the user"""
        self._logger.info("Registering dynamic routes")
        for consumer, producer, doc, func, _type in API.functions:
            self._register_route(consumer, producer, doc, func, _type)

    def _register_route(self, consumer, producer, doc, func, _type):
        """Register a route"""
        consumer_class = SchemaRegistry.get_schema(consumer)
        producer_class = SchemaRegistry.get_schema(producer)

        self._logger.info(f"Registering route {consumer}:{consumer_class} -> {producer}:{producer_class} ({doc})")
        func_wrapper = lambda message: func(self._base_service, message) \
            if isinstance(message, str) else func(self._base_service, jsonable_encoder(message))
        path = f"/api{f'/{consumer}' if consumer else ''}{f'/{producer}' if producer else ''}"

        func_wrapper.__annotations__ = {'message': consumer_class, 'return': producer_class}
        self._router.add_api_route(path, func_wrapper, methods=[_type], response_model=producer_class, tags=["topics"],
                                   summary=doc)
