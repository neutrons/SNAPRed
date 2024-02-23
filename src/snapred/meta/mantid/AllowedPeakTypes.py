import enum
from typing import Literal, get_args

ALLOWED_PEAK_TYPES = Literal[
    "AsymmetricPearsonVII",
    "BackToBackExponential",
    "Bk2BkExpConvPV",
    "DeltaFunction",
    "ElasticDiffRotDiscreteCircle",
    "ElasticDiffSphere",
    "ElasticIsoRotDiff",
    "ExamplePeakFunction",
    "Gaussian",
    "IkedaCarpenterPV",
    "Lorentzian",
    "PseudoVoigt",
    "Voigt",
]

allowed_peak_type_list = list(get_args(ALLOWED_PEAK_TYPES))


class StrEnum(str, enum.Enum):
    pass


_peakMap = zip([s.upper() for s in allowed_peak_type_list], allowed_peak_type_list)


PeakFunctionEnum = StrEnum("PeakFunctionEnum", _peakMap)

_symmetricPeakList = [allowed_peak_type_list[8], allowed_peak_type_list[10], allowed_peak_type_list[11]]
_symmetricPeakMap = zip([s.upper() for s in _symmetricPeakList], _symmetricPeakList)
SymmetricPeakEnum = StrEnum("SymmetricPeakEnum", _symmetricPeakMap)
