import numpy
from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Dict, List, Optional


from snapred.backend.dao.calibration import CalibrationDefaultRecord
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


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

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
