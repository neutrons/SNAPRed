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
            jsonSchemaDict[k] = schema_json_of(v, title=str(v), indent=2)
        elif _isBaseModel(v) or _isBaseModel(v.__args__[0]):
            jsonSchemaDict[k] = schema_json_of(v, title=str(v), indent=2)
    return jsonSchemaDict


@Singleton
class ApiService(Service):
    serviceDirectory: "ServiceDirectory"

    def __init__(self):
        super().__init__()
        self.serviceDirectory = ServiceDirectory()
        self.registerPath("", self.getValidPaths)
        self.registerPath("parameters", self.getPathParameters)
        self.apiCache = None
        return

    @staticmethod
    def name():
        return "api"

    def getPathParameters(self, path: str):
        mainPath = path.split("/")[0]
        subPath = "/".join(path.split("/")[1:])
        if self.apiCache is not None:
            return self.apiCache[mainPath][subPath]
        service = self.serviceDirectory[mainPath]
        func = service.getPaths()[subPath]
        argMap = _parametersToDict(inspect.signature(func).parameters)
        argMap = _convertToJsonSchema(argMap)
        if len(argMap) < 1:
            argMap = None
        return argMap

    def getValidPaths(self):
        # for every path in serviceDirectory and every path register on each service
        # form a tree of paths and expected inputs
        if self.apiCache is not None:
            return self.apiCache
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
        # logger.debug("Valid Paths: {}".format(pathDict))
        self.apiCache = pathDict
        return pathDict
