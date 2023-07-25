# import regex

from snapred.backend.error.UserException import UserException
from snapred.backend.service.ApiService import ApiService
from snapred.backend.service.CalibrantSampleService import CalibrantSampleService
from snapred.backend.service.CalibrationService import CalibrationService

# cant think of a good way around requireing the services to be imported
# here in order to autoregister them
from snapred.backend.service.ConfigLookupService import ConfigLookupService
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.FitMultiplePeakService import FitMultiplePeaksService
from snapred.backend.service.ReductionService import ReductionService
from snapred.backend.service.ServiceDirectory import ServiceDirectory
from snapred.backend.service.SmoothDataExcludingPeaksService import SmoothDataExcludingPeaksService
from snapred.backend.service.StateIdLookupService import StateIdLookupService
from snapred.backend.service.InitializeCalibrationServiceCheck import InitializeCalibrationCheck
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton


# singleton ServiceFactory class
@Singleton
class ServiceFactory:
    serviceDirectory: ServiceDirectory = ServiceDirectory()
    _pathDelimiter = Config["orchestration.path.delimiter"]

    def __init__(self):
        # register the services
        self.serviceDirectory.registerService(ConfigLookupService())
        self.serviceDirectory.registerService(ReductionService())
        self.serviceDirectory.registerService(StateIdLookupService())
        self.serviceDirectory.registerService(CalibrationService())
        self.serviceDirectory.registerService(CrystallographicInfoService())
        self.serviceDirectory.registerService(CalibrantSampleService())
        self.serviceDirectory.registerService(ApiService())
        self.serviceDirectory.registerService(FitMultiplePeaksService())
        self.serviceDirectory.registerService(SmoothDataExcludingPeaksService())
        self.serviceDirectory.registerService(InitializeCalibrationCheck())

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
