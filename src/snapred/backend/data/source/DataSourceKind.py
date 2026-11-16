from snapred.meta.Enum import StrEnum


class DataSourceKind(StrEnum):
    """The kind of a data source: recorded, in order, in the provenance of the data that it produces."""

    NEXUS_FILE = "nexusFile"
    LITE_FILE = "liteFile"
    LIVE = "live"
    REGISTERED = "registered"
    LITE_CONVERSION = "liteConversion"
    CALIBRATION_RECORD = "calibrationRecord"
    CALIBRATION_FILE = "calibrationFile"
    NORMALIZATION_RECORD = "normalizationRecord"
