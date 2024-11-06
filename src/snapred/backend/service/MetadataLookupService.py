from mantid.kernel import IntArrayMandatoryValidator, IntArrayProperty

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
    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()

        # 'DataFactoryService' is a singleton:
        #   declaring it as an instance attribute, instead of a class attribute,
        #   allows singleton reset during testing.
        self.dataFactoryService = DataFactoryService()

        self.registerPath("", self.verifyMultipleRuns)
        return

    @staticmethod
    def name():
        return "verifiedRuns"

    @FromString
    def verifyMultipleRuns(self, runs: str):
        maxRuns = Config["instrument.maxNumberOfRuns"]
        try:
            iap = IntArrayProperty(
                name="RunList",
                values=runs,
                validator=IntArrayMandatoryValidator(),
            )
            if iap.isValid != "":
                raise ValueError(f"Input of {runs} is not valid")
            allRuns = iap.value
            validRuns = []
            for run in allRuns:
                if RunNumberValidator.validateRunNumber(str(run)):
                    validRuns.append(run)
            if len(validRuns) > maxRuns:
                raise ValueError(f"Maximum value of {maxRuns} run numbers exceeded")

            return validRuns
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"Input of {runs} is poorly formatted, see the full error:\n{e}")
