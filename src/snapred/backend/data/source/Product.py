from snapred.meta.Enum import StrEnum


class Product(StrEnum):
    """The kind of data that a source provides.  Each product has its own registry of sources, per resolution."""

    NEUTRON = "neutron"
    DIFFCAL_TABLE = "diffcalTable"
    DIFFCAL_MASK = "diffcalMask"
    NORMALIZATION = "normalization"
    GROUPING = "grouping"
    REDUCTION_PIXEL_MASK = "reductionPixelMask"
