import enum


class PeakFunctionEnum(str, enum.Enum):
    GAUSSIAN = "Gaussian"
    LORENTZIAN = "Lorentzian"
    PSEUDO_VOIGT = "PseudoVoigt"
