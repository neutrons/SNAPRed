from typing import List, Optional

from pydantic import BaseModel


class NormalizationCalibrant(BaseModel):
    numAnnuli: int
    numSlices: Optional[int] = None
    attenuationCrossSection: float
    attenuationHeight: float
    geometry: Optional[str] = None
    FWHM: List[List[int]]
    mask: str
    material: Optional[str] = None
    peaks: List[float]
    radius: float
    sampleNumberDensity: float
    scatteringCrossSection: float
    smoothPoints: int
    calibrationState: Optional[str] = None
