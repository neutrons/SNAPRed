from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.meta.Config import Config


class CalibrationIndexRequest(BaseModel):
    """
    Request an index of all calibration records corresponding to
    the instrument state associated with a given run
    """

    run: RunConfig
