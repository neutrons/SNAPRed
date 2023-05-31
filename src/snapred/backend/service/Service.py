from abc import ABC, abstractmethod
from typing import Any, Dict

from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.meta.Config import Config


class Service(ABC):
    _pathDelimiter = Config["orchestration.path.delimiter"]

    def __init__(self):
        self._paths: Dict[str, Any] = {}

    @abstractmethod
    def name(self):
        pass

    def getPaths(self):
        return self._paths

    def registerPath(self, path, route):
        self._paths[path] = route

    def parsePath(self, path):
        values = path.split(self._pathDelimiter)
        return values[1] if len(values) > 1 else ""

    def orchestrateRecipe(self, request: SNAPRequest):
        path = self.parsePath(request.path)
        route = self._paths.get(path, None)

        if route is None:
            raise ValueError("Path not found: " + path)

        retValue = route(request.payload) if request.payload is not None else route()
        return retValue
