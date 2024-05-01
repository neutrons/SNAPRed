from typing import List, Dict

from pydantic import BaseModel, root_validator

from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.dao.ObjectSHA import ObjectSHA

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
    def check_state_id(cls, values):
        cal = values.get('calibration')
        norm = values.get('normalization')
        stateID = values.get('stateId')

        # Compute SHA for calibration and normalization
        calSHA = ObjectSHA.fromObject(cal)
        normSHA = ObjectSHA.fromObject(norm)

        if not (stateID == calSHA.hex and stateID == normSHA.hex):
            raise ValueError("State ID does not match SHA values from calibration and normalization.")

        # Validate run numbers
        calRunSHA = ObjectSHA.fromObject({'runNumber': cal.runNumber})
        normRunSHA = ObjectSHA.fromObject({'runNumber': norm.runNumber})
        if calRunSHA.hex != normRunSHA.hex:
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