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
