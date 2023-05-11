# import regex
from typing import Any, Dict

from snapred.backend.error.UserException import UserException
from snapred.backend.service.CalibrationService import CalibrationService

# cant think of a good way around requireing the services to be imported
# here in order to autoregister them
from snapred.backend.service.ConfigLookupService import ConfigLookupService
from snapred.backend.service.ExtractionService import ExtractionService
from snapred.backend.service.ReductionService import ReductionService
from snapred.backend.service.StateIdLookupService import StateIdLookupService
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton


# singleton ServiceFactory class
@Singleton
class ServiceFactory:
    services: Dict[str, Any] = {}
    _pathDelimiter = Config["orchestration.path.delimiter"]

    def __init__(self):
        # register the services
        self.registerService(ConfigLookupService())
        self.registerService(ReductionService())
        self.registerService(StateIdLookupService())
        self.registerService(ExtractionService())
        self.registerService(CalibrationService())

    def registerService(self, service):
        # register the service
        serviceName = service.name()

        self.services[serviceName] = service

    def getServiceNames(self):
        return self.services.keys()

    def getService(self, serviceName):
        if serviceName.startswith(self._pathDelimiter):
            serviceName = serviceName[1:]
        serviceName = serviceName.split(self._pathDelimiter)[0]
        service = self.services.get(serviceName, None)
        if service is None:
            raise UserException("Service not found: " + serviceName)
        return service
