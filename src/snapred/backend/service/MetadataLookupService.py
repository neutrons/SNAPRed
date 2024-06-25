from mantid.kernel import IntArrayProperty

from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.service.Service import Service
from snapred.meta.Config import Config
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.validator.RunNumberValidator import RunNumberValidator

logger = snapredLogger.getLogger(__name__)


@Singleton
class MetadataLookupService(Service):
    dataFactoryService = "DataFactoryService"

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.registerPath("", self.verifyMultipleRuns)
        return

    @staticmethod
    def name():
        return "verifiedRuns"

    @FromString
    def verifyMultipleRuns(self, runs: str):
        maxRuns = Config["instrument.maxNumberOfRuns"]
        allRuns = IntArrayProperty(name="RunList", values=runs).value
        validRuns = []
        for run in allRuns:
            if RunNumberValidator.validateRunNumber(str(run)):
                validRuns.append(run)
        if len(validRuns) > maxRuns:
            logger.warning(f"Maximum value of {maxRuns} run numbers exceeded")
            validRuns = []

        return validRuns
