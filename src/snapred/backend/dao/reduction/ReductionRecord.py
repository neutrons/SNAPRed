from typing import Any, Dict, List, Optional

from pydantic import (
    Field,
    field_validator,
    model_validator,
)

from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class ReductionRecord(Record):
    """
    This class plays a fundamental role in the data reduction process, central to refining and
    transforming raw experimental data into a format suitable for scientific analysis. It acts as
    a detailed repository or record for capturing the entirety of the reduction steps, encompassing
    calibration, normalization, and pixel grouping details.
    """

    # inherits from Record
    # - useLiteMode
    # - version
    # specially override this for the case of reduction
    runNumber: Optional[str] = Field(None, exclude=True)

    # specific to reduction records
    runNumbers: List[str]
    calibration: CalibrationRecord
    normalization: NormalizationRecord
    pixelGroupingParameters: Dict[str, List[PixelGroupingParameters]]

    stateId: ObjectSHA
    workspaceNames: List[WorkspaceName]

    def __init__(self, **kwargs):
        # this special init will set runNumber from the runNumber list,
        # while ensuring users cannot set runNumber directly.
        if kwargs.get("runNumber") is not None:
            raise ValueError(
                "runNumber (singular) cannot be set on Reduction records; set the runNumbers list instead."
            )
        kwargs["runNumber"] = kwargs["runNumbers"][0]
        super().__init__(**kwargs)

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
            raise ValueError("Calibration, normalization, and reduction records are not from the same state.")

        return self

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
