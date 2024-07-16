from typing import Optional

from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters


class Calibration(CalculationParameters):
    """

    The Calibration class acts as a container for parameters primarily utilized in fitting processes within the context
    of scientific data analysis. It encompasses static details such as the instrumentState indicating the condition of
    the instrument at the time of calibration, seedRun for identifying the initial data set, creationDate marking when
    the calibration was created, along with a name and a default version number.

    """

    # inherits from CalculationParameters
    # - instrumentState: InstrumentState
    # - seedRun: str
    # - useLiteMode: bool
    # - creationDate: datetime
    # - name: str
    # - version: Union[int, DEFAULT, UNINITIALIZED]

    # these are saved for later use in reduction
    calibrantSamplePath: Optional[str] = None
    peakIntensityThreshold: Optional[float] = None
