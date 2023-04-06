class API:
    """API class to register functions to be exposed as API endpoints."""
    functions = []

    @staticmethod
    def post(consumer=None, producer=None, doc=None):
        def decorator(func):
            func.consumer = consumer
            func.producer = producer
            func.doc = doc
            API.functions.append((consumer, producer, doc, func, "POST"))
            return func

        return decorator

    @staticmethod
    def get(consumer=None, producer=None, doc=None):
        def decorator(func):
            func.consumer = consumer
            func.producer = producer
            func.doc = doc
            API.functions.append((consumer, producer, doc, func, "GET"))
            return func

        return decorator

    @staticmethod
    def get_func_by_consumer(consumer):
        func_list = []
        for func in API.functions:
            if func[0] == consumer:
                func_list.append(func)
        return func_list
