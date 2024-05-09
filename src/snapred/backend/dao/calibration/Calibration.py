from datetime import datetime

from pydantic import BaseModel

from snapred.backend.dao.state import InstrumentState


class Calibration(BaseModel):
    """

    The Calibration class acts as a container for parameters primarily utilized in fitting processes within the context
    of scientific data analysis. It encompasses static details such as the instrumentState indicating the condition of
    the instrument at the time of calibration, seedRun for identifying the initial data set, creationDate marking when
    the calibration was created, along with a name and a default version number.

    """

    instrumentState: InstrumentState
    seedRun: int
    useLiteMode: bool
    creationDate: datetime
    name: str
    version: int = 0
