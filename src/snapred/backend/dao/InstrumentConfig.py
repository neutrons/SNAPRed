from pathlib import Path

from pydantic import BaseModel


class InstrumentConfig(BaseModel):
    """Class to hold the instrument parameters."""

    version: str
    facility: str
    name: str
    nexusFileExtension: str
    nexusFilePrefix: str
    calibrationFileExtension: str
    calibrationFilePrefix: str
    calibrationDirectory: Path
    pixelGroupingDirectory: str
    sharedDirectory: str
    nexusDirectory: str
    reducedDataDirectory: str
    reductionRecordDirectory: str
    bandwidth: float
    maxBandwidth: float
    L1: float
    L2: float
    delTOverT: float
    delLOverL: float
    delThNoGuide: float
    delThWithGuide: float
    NBins: int = 10
