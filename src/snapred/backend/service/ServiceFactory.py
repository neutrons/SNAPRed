from snapred.meta.Singleton import Singleton

# cant think of a good way around requireing the services to be imported
# here in order to autoregister them
from snapred.backend.service.ConfigLookupService import ConfigLookupService
from snapred.backend.service.ReductionService import ReductionService
from snapred.backend.service.StateIdLookupService import StateIdLookupService

# import regex
import re

# singleton ServiceFactory class
@Singleton
class ServiceFactory:
    services = {}

    def __init__(self):
        # register the services
        self.registerService(ConfigLookupService())
        self.registerService(ReductionService())
        self.registerService(StateIdLookupService())

    def registerService(self, service):
        # register the service
        serviceName = service.__class__.__name__
        # map human readable modes to backend services
        # remove string Service from the end of the class name
        if serviceName.endswith('Service'):
            serviceName = serviceName[:-7]
        # Add Spaces after sets of capital letters
        serviceName = re.sub('([A-Z][a-z]+)', r' \1', serviceName).strip()
        self.services[serviceName] = service

    def getServiceNames(self):
        return self.services.keys()

    def getService(self, serviceName):
        return self.services[serviceName]