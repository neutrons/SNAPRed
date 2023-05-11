from abc import ABC, abstractmethod
from typing import Any, Dict

from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.meta.Config import Config


class Service(ABC):
    _pathDelimiter = Config["orchestration.path.delimiter"]
    _paths: Dict[str, Any] = {}

    @abstractmethod
    def name(self):
        pass

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

        return route(request.payload)
