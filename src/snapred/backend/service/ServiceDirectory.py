import inspect
from typing import Any, Dict

from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class ServiceDirectory:
    _services: Dict[str, Any] = {}

    @classproperty
    def _pathDelimiter(cls):
        return Config["orchestration.path.delimiter"]

    def registerService(self, service):
        # register the service
        serviceName = service.name()
        self._services[serviceName] = service

    def __contains__(self, key):
        return key in self._services

    def __getitem__(self, key):
        # convert the held value from class to instance
        value = self._services[key]
        if inspect.isclass(value):
            self._services[key] = value()
            return self._services[key]
        else:
            return value

    def asDict(self):
        return self._services

    def keys(self):
        return self._services.keys()

    def get(self, key, default=None):
        if key in self:
            return self[key]
        else:
            return default
