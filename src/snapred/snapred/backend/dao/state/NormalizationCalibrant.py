from typing import List, Optional

from pydantic import BaseModel


class NormalizationCalibrant(BaseModel):
    numAnnuli: int
    numSlices: Optional[int]
    attenuationCrossSection: float
    attenuationHeight: float
    geometry: Optional[str]
    FWHM: List[List[int]]
    mask: str
    material: Optional[str]
    peaks: List[float]
    radius: float
    sampleNumberDensity: float
    scatteringCrossSection: float
    smoothPoints: int
    calibrationState: Optional[str]
