from threading import Thread


class SubProcess(Thread):
    """Base class for all sub processes"""

    def __init__(self):
        super().__init__()
        self.daemon = True
        self.running = True

        self._callback = None
        self._base_service = None

    @property
    def callback(self):
        return self._callback

    @callback.setter
    def callback(self, value):
        self._callback = value

    @property
    def base_service(self):
        return self._base_service

    @base_service.setter
    def base_service(self, value):
        self._base_service = value

    def stop(self):
        """Stop the service"""
        self.running = False

    def is_running(self):
        return self.running

    def run(self):
        raise NotImplementedError
