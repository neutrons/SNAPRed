# import regex
import re
from typing import Any, Dict

from snapred.backend.error.UserException import UserException

# cant think of a good way around requireing the services to be imported
# here in order to autoregister them
from snapred.backend.service.ConfigLookupService import ConfigLookupService
from snapred.backend.service.ExtractionService import ExtractionService
from snapred.backend.service.ReductionService import ReductionService
from snapred.backend.service.StateIdLookupService import StateIdLookupService
from snapred.meta.Singleton import Singleton


# singleton ServiceFactory class
@Singleton
class ServiceFactory:
    services: Dict[str, Any] = {}

    def __init__(self):
        # register the services
        self.registerService(ConfigLookupService())
        self.registerService(ReductionService())
        self.registerService(StateIdLookupService())
        self.registerService(ExtractionService())

    def registerService(self, service):
        # register the service
        serviceName = service.__class__.__name__
        # map human readable modes to backend services
        # remove string Service from the end of the class name
        if serviceName.endswith("Service"):
            serviceName = serviceName[:-7]
        # Add Spaces after sets of capital letters
        serviceName = re.sub("([A-Z][a-z]+)", r" \1", serviceName).strip()
        self.services[serviceName] = service

    def getServiceNames(self):
        return self.services.keys()

    def getService(self, serviceName):
        service = self.services.get(serviceName, None)
        if service is None:
            raise UserException("Service not found: " + serviceName)
        return service
