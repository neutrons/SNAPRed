from typing import Any, Dict

from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class ServiceDirectory:
    _services: Dict[str, Any] = {}
    _pathDelimiter = Config["orchestration.path.delimiter"]

    def registerService(self, service):
        # register the service
        serviceName = service.name()
        self._services[serviceName] = service

    def __getitem__(self, key):
        return self._services[key]

    def asDict(self):
        return self._services

    def keys(self):
        return self._services.keys()

    def get(self, key, default):
        return self._services.get(key, default)
