from environs import Env

_env = Env()


class ENV:
    """Environment variables"""

    # LOGGING
    LOG_LEVEL = _env('LOG_LEVEL', 'INFO')
    DEBUG = _env.bool("DEBUG", False)

    # TOPICS
    CONSUME = [c.strip() for c in _env('CONSUME', '').split(',') if c.strip()]
    PRODUCE = [p.strip() for p in _env('PRODUCE', '').split(',') if p.strip()]
    CLIENT_ID = _env('CLIENT_ID', None)

    # KAFKA
    KAFKA_HOST = _env('KAFKA_HOST', '127.0.0.1:3501')
    SCHEMA_REGISTRY = _env('SCHEMA_REGISTRY', 'http://localhost:3502')
    PARTITIONER = _env("PARTITIONER", "random")
    MESSAGE_MAX_BYTES = _env.int('MESSAGE_MAX_BYTES', 1000000)
    HEARTBEAT_INTERVAL = _env.int('HEARTBEAT_INTERVAL', 10)
    OFFSET_TYPE = _env('OFFSET_TYPE', 'earliest')
    STRING_BASED_KEYS = _env.bool('STRING_BASED_KEYS', True)

    # REST API
    REST_API_ENABLED = _env.bool('REST_API_ENABLED', True)
    REST_API_PORT = _env.int('REST_API_PORT', 8080)
    REST_API_HOST = _env('REST_API_HOST', '0.0.0.0')
    REST_LOG_MESSAGES = _env.bool('REST_LOG_MESSAGES', False)

    # OTHER
    LOCAL_SCHEMA_REGISTRY_ENABLED = _env.bool('LOCAL_SCHEMA_REGISTRY_ENABLED', True)

    def update(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
