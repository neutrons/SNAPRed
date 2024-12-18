from pydantic import BaseModel, ConfigDict 

from snapred.meta.Config import Config


class InstrumentConfig(BaseModel):
    """Class to hold the instrument parameters."""

    version: str
    facility: str
    name: str
    nexusFileExtension: str
    nexusFilePrefix: str
    calibrationFileExtension: str
    calibrationFilePrefix: str
    calibrationDirectory: str
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
    width: float
    frequency: float
    lowWavelengthCrop: float = Config["constants.CropFactors.lowWavelengthCrop"]
    
    model_config = ConfigDict(
        strict=True
    )
