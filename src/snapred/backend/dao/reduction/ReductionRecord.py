from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from snapred.backend.dao.calibration import CalibrationDefaultRecord
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.Hook import Hook
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.Time import isoFromTimestamp


class ReductionRecord(BaseModel):
    """
    This class plays a fundamental role in the data reduction process, central to refining and
    transforming raw experimental data into a format suitable for scientific analysis. It acts as
    a detailed repository or record for capturing the entirety of the reduction steps, encompassing
    calibration, normalization, and pixel grouping details.
    """

    # Reduction has distinct registration attributes from Calibration and Normalization.
    #   for this reason, this class is not derived from 'Record',
    #   and does not include a 'CalculationParameters' instance.
    runNumber: str
    useLiteMode: bool | numpy.bool_
    timestamp: float = Field(frozen=True, default=None)

    # specific to reduction records
    calibration: Optional[CalibrationRecord | CalibrationDefaultRecord] = None
    normalization: Optional[NormalizationRecord] = None
    pixelGroupingParameters: Dict[str, List[PixelGroupingParameters]]

    workspaceNames: List[WorkspaceName]

    alternativeCalibrationFilePath: Optional[Path] = None
    hooks: Dict[str, List[Hook]] | None = None

    snapredVersion: str = "unknown"
    snapwrapVersion: Optional[str] = "unknown"

    """
    *Other details to include above*:

    containerType:
    backgroundType:
    backgroundData:
    containerMasks:
    geometricProperties:
    crystallographicProperties:
    materialProperties:
    attenuationType:
    attenuationData:
    attenuationParameters:

    *Details of post reduction processing*:

    dSAppliedData: (dS == down sampling)
    dSAppliedParameters: (rebinning parameters)
    """

    @field_validator("useLiteMode", mode="before")
    @classmethod
    def validate_useLiteMode(cls, v):
        # this allows the use of 'strict' mode
        if isinstance(v, numpy.bool_):
            v = bool(v)
        return v

    @field_serializer("snapredVersion")
    @classmethod
    def serialize_snapredVersion(cls, v: str) -> str:
        if v is None or v == "" or v == "unknown":
            raise ValueError(
                "snapredVersion is required for ReductionRecord serialization.",
            )
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v):
        if isinstance(v, datetime):
            # Convert datetime to timestamp
            v = v.timestamp()
        if isinstance(v, str):
            import numpy as np

            # Convert ISO format string to timestamp
            v = np.datetime64(v).astype(int) / 1e9  # convert to seconds
        if isinstance(v, int):
            # support legacy integer encoding
            return float(v) / 1000.0
        if not isinstance(v, float):
            raise ValueError("timestamp must be a float, int, or ISO format string")
        return float(v)

    @field_serializer("timestamp")
    @classmethod
    def serialize_timestamp(cls, v):
        return isoFromTimestamp(v)

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
