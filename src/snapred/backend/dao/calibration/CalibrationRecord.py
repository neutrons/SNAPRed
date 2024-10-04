from typing import List, Optional

from snapred.backend.dao.calibration.CalibrationDefaultRecord import CalibrationDefaultRecord
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.PixelGroup import PixelGroup


class CalibrationRecord(CalibrationDefaultRecord, extra="ignore"):
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

    # specific to full calibration records
    crystalInfo: CrystallographicInfo
    pixelGroups: Optional[List[PixelGroup]] = None  # TODO: really shouldn't be optional, will be when sns data fixed
    focusGroupCalibrationMetrics: FocusGroupMetric
