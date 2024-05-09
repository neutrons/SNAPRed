from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig


class CalibrationIndexRequest(BaseModel):
    """

    The CalibrationIndexRequest class is designed to facilitate the retrieval of calibration
    records that match the instrument state of a specific run. By including a run configuration,
    it allows users to query calibration data relevant to the conditions and settings of a
    particular experimental run.

    """

    run: RunConfig
