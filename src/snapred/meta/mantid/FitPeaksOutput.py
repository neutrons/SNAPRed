from enum import Enum

"""
NOTE child workspaces from a workspace group can ONLY be accessed by index,
and not by anything reasonable like the name.  Therefore the order of these
elements is critically important.  They MUST be in alphabetical order by suffix, as
alphabetical ordering is the only way to impose consistent order of the workspaces.
"""

"""
NOTE PDCalibration using the suffix '_dspacing', whereas FitPeaks prefers
the suffix '_peakpos'. The easiest solution is to change the FitPeaks output
to match PDCalibration, '_dspacing'.
"""


class FitOutputEnum(Enum):
    PeakPosition = 0
    ParameterError = 1
    Parameters = 2
    Workspace = 3


FIT_PEAK_DIAG_SUFFIX = {
    FitOutputEnum.PeakPosition: "_dspacing",
    FitOutputEnum.ParameterError: "_fiterror",
    FitOutputEnum.Parameters: "_fitparam",
    FitOutputEnum.Workspace: "_fitted",
}
