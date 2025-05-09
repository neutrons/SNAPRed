from snapred.backend.error.UserException import UserException
from snapred.backend.service.ApiService import ApiService
from snapred.backend.service.CalibrantSampleService import CalibrantSampleService
from snapred.backend.service.CalibrationService import CalibrationService

# I can't think of a good way around requiring the services to be imported
#   here in order to auto-register them.
from snapred.backend.service.ConfigLookupService import ConfigLookupService
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.LiteDataService import LiteDataService
from snapred.backend.service.NormalizationService import NormalizationService
from snapred.backend.service.ReductionService import ReductionService
from snapred.backend.service.ServiceDirectory import ServiceDirectory
from snapred.backend.service.StateIdLookupService import StateIdLookupService
from snapred.backend.service.WorkspaceMetadataService import WorkspaceMetadataService
from snapred.backend.service.WorkspaceService import WorkspaceService
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.decorators.Singleton import Singleton


# Singleton ServiceFactory class
@Singleton
class ServiceFactory:
    serviceDirectory: ServiceDirectory = ServiceDirectory()

    def __init__(self):
        # register the services
        self.serviceDirectory.registerService(ConfigLookupService)
        self.serviceDirectory.registerService(ReductionService)
        self.serviceDirectory.registerService(StateIdLookupService)
        self.serviceDirectory.registerService(CalibrationService)
        self.serviceDirectory.registerService(CrystallographicInfoService)
        self.serviceDirectory.registerService(CalibrantSampleService)
        self.serviceDirectory.registerService(ApiService)
        self.serviceDirectory.registerService(LiteDataService)
        self.serviceDirectory.registerService(NormalizationService)
        self.serviceDirectory.registerService(WorkspaceService)
        self.serviceDirectory.registerService(WorkspaceMetadataService)

    @classproperty
    def _pathDelimiter(cls):
        return Config["orchestration.path.delimiter"]

    def getServiceNames(self):
        return self.serviceDirectory.keys()

    def getService(self, serviceName):
        if serviceName.startswith(self._pathDelimiter):
            serviceName = serviceName[1:]
        serviceName = serviceName.split(self._pathDelimiter)[0]
        service = self.serviceDirectory.get(serviceName, None)
        if service is None:
            raise UserException("Service not found: " + serviceName)
        return service
