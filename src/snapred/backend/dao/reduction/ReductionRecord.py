from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class ReductionRecord(BaseModel):
    """
    This class plays a fundamental role in the data reduction process, central to refining and
    transforming raw experimental data into a format suitable for scientific analysis. It acts as
    a detailed repository or record for capturing the entirety of the reduction steps, encompassing
    calibration, normalization, and pixel grouping details.
    """

    runNumbers: List[str]
    useLiteMode: bool
    calibration: CalibrationRecord
    normalization: NormalizationRecord
    pixelGroupingParameters: Dict[str, List[PixelGroupingParameters]]
    version: int
    stateId: ObjectSHA
    workspaceNames: List[WorkspaceName]

    @field_validator("stateId", mode="before")
    @classmethod
    def str_to_ObjectSHA(cls, v: Any) -> Any:
        # ObjectSHA to be stored in JSON as _only_ a single hex string, for the hex digest itself
        if isinstance(v, str):
            return ObjectSHA(hex=v, decodedKey=None)
        return v

    @model_validator(mode="after")
    def checkStateId(self):
        cal = self.calibration
        norm = self.normalization
        redStateId = self.stateId

        calStateId = cal.calibrationFittingIngredients.instrumentState.id
        normStateId = norm.calibration.instrumentState.id
        if not (calStateId == normStateId == redStateId):
            raise ValidationError("Calibration, normalization, and reduction records are not from the same state.")

        return self

    model_config = ConfigDict(
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )

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
