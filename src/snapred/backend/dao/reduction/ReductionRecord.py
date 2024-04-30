from typing import List

from pydantic import BaseModel

from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters

class ReductionRecord(BaseModel):
    """ 
    
    This class plays a fundamental role in the data reduction process, central to refining and 
    transforming raw experimental data into a format suitable for scientific analysis. It acts as 
    a detailed repository or record for capturing the entirety of the reduction steps, encompassing
    calibration, normalization, and pixel grouping details.

    """
    runNumbers: List[str]
    isLite: bool
    calibrationRecord: CalibrationRecord
    normalizationRecord: NormalizationRecord
    pixelGroupingParameters: List[PixelGroupingParameters]

    # placeholders for later phase 3 additions (bound to change)

    """

    *Other details to include above(?)*:

    workspaceNames:

    *Details of the reduction*:

    containerType:
    backgroundType:
    backgroundData:
    containerMasks:
    geometricProperties:
    crystallographicProperties:
    materialProperties:
    attenuationType:
    attenuationData:
    attenuationsParameters:


    *Details of post reduction processing*:

    dSAppliedData: (dS == down sampling)
    dSAppliedParameters: (rebinning parameters)

    """