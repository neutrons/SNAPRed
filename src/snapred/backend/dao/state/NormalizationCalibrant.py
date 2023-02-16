from typing import List
from dataclasses import dataclass



# https://docs.python.org/3/library/dataclasses.html
@dataclass
class NormalizationCalibrant:
    numAnnuli: int
    numSlices: int
    attenuationCrossSection: float
    attenuationHeight: float
    geometry: str
    FWHM: List[int]
    mask: str
    material: str
    peaks: List[float]
    radius: float
    sampleNumberDensity: float
    scatteringCrossSection: float
    smoothPoints: int
    calibrationState: str