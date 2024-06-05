from pydantic import BaseModel, Extra

from snapred.backend.dao.IndexEntry import UNINITIALIZED, Version


class Record(BaseModel, extra=Extra.allow):
    """

    The CalibrationRecord class, serves as a comprehensive log of the inputs and parameters employed
    to produce a specific version of a calibration. It systematically records the runNumber, detailed
    crystalInfo from CrystallographicInfo, and the calibration parameters from Calibration. Additionally,
    it may include pixelGroups, a list of PixelGroup objects (intended to be mandatory in future updates),
    and focusGroupCalibrationMetrics derived from FocusGroupMetric to evaluate calibration quality. The
    workspaces dictionary maps WorkspaceType to lists of WorkspaceName, organizing the associated Mantid
    workspace names by type. An optional version number allows for tracking the calibration evolution over
    time.

    """

    runNumber: str
    useLiteMode: bool
    version: Version = UNINITIALIZED


Nonrecord = Record(
    runNumber="none",
    useLiteMode=False,
    version=UNINITIALIZED,
)
