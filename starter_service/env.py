from environs import Env

_env = Env()
_supported_types = [str, int, float, bool]


class ENV:
    """Environment variables"""

    # LOGGING
    LOG_LEVEL = _env('LOG_LEVEL', 'INFO')
    DEBUG = _env.bool("DEBUG", False)

    # TOPICS
    CONSUME = _env('CONSUME', '')
    PRODUCE = _env('PRODUCE', '')
    CLIENT_ID = _env('CLIENT_ID', None)

    # KAFKA
    KAFKA_HOST = _env('KAFKA_HOST', '127.0.0.1:3501')
    SCHEMA_REGISTRY = _env('SCHEMA_REGISTRY', 'http://localhost:3502')
    PARTITIONER = _env("PARTITIONER", "random")
    MESSAGE_MAX_BYTES = _env.int('MESSAGE_MAX_BYTES', 1000000)
    HEARTBEAT_INTERVAL = _env.int('HEARTBEAT_INTERVAL', 10)
    STRING_BASED_KEYS = _env.bool('STRING_BASED_KEYS', True)

    OFFSET_TYPE = _env('OFFSET_TYPE', 'latest')
    IGNORE_TIMEOUT = _env("IGNORE_TIMEOUT", None)
    USE_LATEST = _env.bool("USE_LATEST", False)

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

    @classmethod
    def SET(cls, name, value, type=None):
        if type and type in _supported_types:
            _val = type(_env(name, value))
        else:
            _val = _env(name, value)
        setattr(cls, name, _val)

    @classmethod
    def GET(cls, name, default=None, type=None):
        if not hasattr(cls, name):
            cls.SET(name, default, type)
        return getattr(cls, name)

    def update(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
