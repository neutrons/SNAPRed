import inspect
from typing import get_origin

from pydantic import BaseModel, schema_json_of

from snapred.backend.log.logger import snapredLogger
from snapred.backend.service.Service import Service
from snapred.backend.service.ServiceDirectory import ServiceDirectory
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


def _isBaseModel(clazz):
    return inspect.isclass(clazz) and issubclass(clazz, BaseModel)


def _parametersToDict(parameters):
    return {k: v.annotation for k, v in parameters.items()}


def _convertToJsonSchema(parameterDic):
    jsonSchemaDict = {}
    for k, v in parameterDic.items():
        innerType = get_origin(v)
        if not _isBaseModel(v) and innerType is None:
            jsonSchemaDict[k] = str(v)
        elif _isBaseModel(v) or _isBaseModel(v.__args__[0]):
            jsonSchemaDict[k] = schema_json_of(v, title=str(v), indent=2)
    return jsonSchemaDict


@Singleton
class ApiService(Service):
    _name = "api"
    serviceDirectory: ServiceDirectory = ServiceDirectory()

    def __init__(self):
        super().__init__()
        self.registerPath("", self.getValidPaths)
        return

    def name(self):
        return self._name

    def getValidPaths(self):
        # for every path in serviceDirectory and every path register on each service
        # form a tree of paths and expected inputs
        mainPaths = self.serviceDirectory.keys()
        pathDict = {}
        for path in mainPaths:
            service = self.serviceDirectory[path]
            subPaths = service.getPaths()
            subpathDict = {}
            # import pdb; pdb.set_trace()
            for subPath, func in subPaths.items():
                argMap = _parametersToDict(inspect.signature(func).parameters)
                argMap = _convertToJsonSchema(argMap)
                # for each arg that is a BaseModel, convert it to its schema
                if len(argMap) < 1:
                    argMap = None
                subpathDict[subPath] = argMap
            if len(subpathDict) < 1:
                subpathDict = None
            pathDict[path] = subpathDict
        logger.debug("Valid Paths: {}".format(pathDict))
        return pathDict
