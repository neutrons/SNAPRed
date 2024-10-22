from typing import Dict

from mantid.api import PythonAlgorithm
from mantid.dataobjects import TableWorkspaceProperty
from mantid.kernel import Direction, FloatPropertyWithValue, ULongLongPropertyWithValue

from snapred.backend.log.logger import snapredLogger
from snapred.meta.pointer import create_pointer

logger = snapredLogger.getLogger(__name__)


class VerifyChiSquared(PythonAlgorithm):
    """
    Given a table workspace with fitting diagnosing information and a maximum threshold for chisq,
    returns a list of good peaks and a list of bad peaks.
    """

    TABLEWKSPPROP = "InputWorkspace"

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def validateInputs(self) -> Dict[str, str]:
        err = {}
        tab = self.getProperty(self.TABLEWKSPPROP).value
        colnames = set(tab.toDict().keys())
        mandatorynames = {"chi2", "wsindex", "centre"}
        errors = []
        for name in mandatorynames:
            if name not in colnames:
                errors.append(name)
        if errors != []:
            err[self.TABLEWKSPPROP] = "Missing mandatory columns: "
            for name in errors:
                err[self.TABLEWKSPPROP] += f"{name}, "
        return err

    def PyInit(self):
        # declare properties
        self.declareProperty(
            TableWorkspaceProperty(self.TABLEWKSPPROP, "", direction=Direction.Input),
            doc="Table workspace from peak-fitting diagnoses.",
        )
        self.declareProperty(
            FloatPropertyWithValue("MaximumChiSquared", 0, direction=Direction.Input),
            doc="The threshold value for chisq to be considered 'good.'",
        )
        self.declareProperty(
            ULongLongPropertyWithValue("GoodPeaks", id(None), direction=Direction.Output),
            doc="Pointer to a list of good peaks (chi2<max), as JSON objects with Spectrum, Peak Location. Chi2.",
        )
        self.declareProperty(
            ULongLongPropertyWithValue("BadPeaks", id(None), direction=Direction.Output),
            doc="Pointer to a list of bad peaks (chi2 >= max), as JSON objects with Spectrum, Peak Location. Chi2.",
        )
        self.declareProperty("LogResults", False, direction=Direction.Input)
        self.setRethrows(True)

    def PyExec(self) -> None:
        maxChiSq = self.getProperty("MaximumChiSquared").value
        tab = self.getProperty(self.TABLEWKSPPROP).value
        tabDict = tab.toDict()
        chi2 = tabDict["chi2"]
        goodPeaks = []
        badPeaks = []
        for index, item in enumerate(chi2):
            peakJSON = {
                "Spectrum": tabDict["wsindex"][index],
                "Peak Location": tabDict["centre"][index],
                "Chi2": tabDict["chi2"][index],
            }
            if item < maxChiSq:
                goodPeaks.append(peakJSON)
            else:
                badPeaks.append(peakJSON)

        if self.getProperty("LogResults").value:
            if len(goodPeaks) < 2:
                logger.warning(
                    f"Insufficient number of well-fitted peaks (chi2 < {maxChiSq})."
                    + "Try to adjust parameters in Tweak Peak Peek tab"
                    + f"Bad peaks info: {badPeaks}"
                )
            else:
                logger.info(f"Sufficient number of well-fitted peaks (chi2 < {maxChiSq}).: {len(goodPeaks)}")

        self.setProperty("GoodPeaks", create_pointer(goodPeaks))
        self.setProperty("BadPeaks", create_pointer(badPeaks))
