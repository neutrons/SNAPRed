from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.state.InstrumentState import InstrumentState

# NOTE: the __init__ loads CalibrationExportRequest, which imports Calibration, which causes
#       a circular import situation.  In the future, need to remove the circualr import
#       so that full set of ingredients can be preserved
# from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.meta.Config import Config


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
    version: int = Config["version.calibration.start"]
