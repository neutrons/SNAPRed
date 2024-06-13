from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Dict, Callable, List

from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.meta.Config import Config
from snapred.backend.dao.SNAPRequest import SNAPRequest

# Type define which is a callable function with a List of SNAPRequests as input,
# and a Dict of str keys and List of SNAPRequests values as expected output.
GroupingLambda = Callable[[List[SNAPRequest]], Dict[str, List[SNAPRequest]]]


class Service(ABC):
    _pathDelimiter = Config["orchestration.path.delimiter"]

    def __init__(self):
        self._paths: Dict[str, Any] = {}
        self._lambdas : Dict[str, List[GroupingLambda]] = {}

    @abstractmethod
    def name(self):
        pass

    def getPaths(self):
        return self._paths

    def registerPath(self, path, route):
        self._paths[path] = route

    def parsePath(self, path):
        if path.startswith(self._pathDelimiter):
            path = path[1:]
        values = path.split(self._pathDelimiter)
        return values[1] if len(values) > 1 else ""

    def orchestrateRecipe(self, request: SNAPRequest):
        path = self.parsePath(request.path)
        route = self._paths.get(path, None)

        if route is None:
            raise ValueError("Path not found: " + path)

        retValue = route(request.payload) if request.payload is not None else route()
        return retValue

    def registerGrouping(self, path: str, groupingLambda: GroupingLambda):
        if self._paths[path] is not None:
            if self._lambdas.get(path) is None:
                self._lambdas[path] = []
            self._lambdas[path].append(groupingLambda)
        else:
            raise ValueError(f"Given path does not exist")

    def getGroupings(self, path: str):        
        return self._lambdas[path]
