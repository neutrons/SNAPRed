from typing import Dict, List

from pydantic import BaseModel, root_validator
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.data.DataFactoryService import DataFactoryService


class ReductionRecord(BaseModel):
    """
    This class plays a fundamental role in the data reduction process, central to refining and
    transforming raw experimental data into a format suitable for scientific analysis. It acts as
    a detailed repository or record for capturing the entirety of the reduction steps, encompassing
    calibration, normalization, and pixel grouping details.
    """

    runNumbers: List[str]
    isLite: bool
    calibration: CalibrationRecord
    normalization: NormalizationRecord
    pixelGroupingParameters: Dict[str, PixelGroupingParameters]
    version: int
    stateId: ObjectSHA
    workspaceNames: List[str]

    @root_validator
    def checkStateId(cls, values):
        cal = values.get("calibration")
        norm = values.get("normalization")
        redStateID = values.get("stateID")

        dataFactoryService = DataFactoryService()

        calStateId, _ = dataFactoryService.constructStateId(cal.runNumber)
        normStateId, _ = dataFactoryService.constructStateId(norm.runNumber)

        if not (calStateId == normStateId == redStateID.hex):
            raise ValueError("Run numbers do not generate the same SHA.")

        return values

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
