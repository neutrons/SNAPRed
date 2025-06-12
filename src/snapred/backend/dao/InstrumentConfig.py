from pydantic import Field

from snapred.backend.dao.indexing.IndexedObject import IndexedObject
from snapred.meta.Config import Config


class InstrumentConfig(IndexedObject):
    """Class to hold the instrument parameters."""

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
    lowWavelengthCrop: float = Field(default_factory=lambda: Config["constants.CropFactors.lowWavelengthCrop"])
