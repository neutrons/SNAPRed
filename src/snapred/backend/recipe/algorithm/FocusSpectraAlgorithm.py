from typing import Dict, Tuple

from mantid.api import (
    AlgorithmFactory,
    ExperimentInfo,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceUnitValidator,
)
from mantid.dataobjects import GroupingWorkspace
from mantid.kernel import Direction, StringMandatoryValidator

from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class FocusSpectraAlgorithm(PythonAlgorithm):
    """
    This algorithm performs diffraction focussing on dSpacing data. It applies
    diffraction focussing using a grouping workspace, which then additionally applies
    the optimal binning constraints using the provided PixelGroup.

    """

    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty(
                "InputWorkspace",
                "",
                Direction.Input,
                PropertyMode.Mandatory,
                validator=WorkspaceUnitValidator("dSpacing"),
            ),
            doc="Workspace containing values at each pixel",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace defining the grouping for diffraction focusing",
        )
        self.declareProperty(
            MatrixWorkspaceProperty(
                "OutputWorkspace",
                "",
                Direction.Output,
                PropertyMode.Mandatory,
                validator=WorkspaceUnitValidator("dSpacing"),
            ),
            doc="The diffraction-focused, and rebinned data",
        )
        self.declareProperty(
            "PixelGroup",
            defaultValue="",
            validator=StringMandatoryValidator(),
            direction=Direction.Input,
        )
        self.declareProperty("PreserveEvents", defaultValue=True, direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self):
        self.pixelGroup: PixelGroup = PixelGroup.model_validate_json(self.getPropertyValue("PixelGroup"))
        self.preserveEvents = self.getProperty("PreserveEvents").value

    def unbagGroceries(self):
        self.inputWSName = self.getPropertyValue("InputWorkspace")
        self.groupingWSName = self.getPropertyValue("GroupingWorkspace")
        self.outputWSName = self.getPropertyValue("OutputWorkspace")

    @staticmethod
    def _appendErrorMessage(errors: Dict[str, str], key: str, msg: str):
        existingMsg = errors.get(key, "")
        if bool(existingMsg):
            existingMsg += "\n"
        errors[key] = existingMsg + msg

    @staticmethod
    def _instrumentSignature(ws: ExperimentInfo) -> Tuple[str, int]:
        # Info to implement a practical test for comparing instruments.
        # (Mantid framework does not provide such a test.)

        # Instrument name:
        name = ws.getInstrument().getName()
        if name.lower().endswith(".xml"):
            name = name[0 : name.rfind(".")]

        # Number of non-monitor pixels:
        N_pixels = ws.getInstrument().getNumberDetectors(True)

        return name, N_pixels

    def validateInputs(self) -> Dict[str, str]:
        errors = {}

        inputWs = self.mantidSnapper.mtd[self.getPropertyValue("InputWorkspace")]

        groupingWs = self.mantidSnapper.mtd[self.getPropertyValue("GroupingWorkspace")]
        if not isinstance(groupingWs, GroupingWorkspace):
            errors["GroupingWorkspace"] = "Grouping workspace must be an instance of `GroupingWorkspace`"

            # EARLY RETURN:
            return errors

        # Make sure that the input-data workspace can be reduced by the grouping workspace.

        # The input-data and grouping workspaces must have the same instrument.
        # Comparing instruments is problematic: however, requiring both the instrument-name,
        #   and the pixel count to match is a fairly safe verification.
        if self._instrumentSignature(inputWs) != self._instrumentSignature(groupingWs):
            msg = "The input-data and grouping workspaces must have the same instrument."
            self._appendErrorMessage(errors, "InputWorkspace", msg)
            self._appendErrorMessage(errors, "GroupingWorkspace", msg)

        # Verify that the input-data hasn't already been focussed.
        if inputWs.getNumberHistograms() == len(groupingWs.getGroupIDs()):
            self._appendErrorMessage(
                errors, "InputWorkspace", "The input data appears to already have been diffraction focussed."
            )

        # Verify that the input-data has one spectrum per [non-monitor] pixel.
        if inputWs.getNumberHistograms() != inputWs.getInstrument().getNumberDetectors(True):
            self._appendErrorMessage(
                errors, "InputWorkspace", "The input workspace must have one spectrum per [non-monitor] pixel."
            )

        # IMPORTANT: There is no requirement that the grouping workspace have one spectrum per detector.
        #   Its spectrum-to-detector map may be used to reduce this number.

        return errors

    def PyExec(self):
        self.chopIngredients()
        self.unbagGroceries()

        self.mantidSnapper.DiffractionFocussing(
            "Performing Diffraction Focussing ...",
            InputWorkspace=self.inputWSName,
            GroupingWorkspace=self.groupingWSName,
            OutputWorkspace=self.outputWSName,
            PreserveEvents=self.preserveEvents,
            DMin=self.pixelGroup.dMin(),
            DMax=self.pixelGroup.dMax(),
            Delta=self.pixelGroup.dBin(),
            FullBinsOnly=True,
        )
        # With these arguments, output from `DiffractionFocussing` will now have ragged binning,
        #   so the former call to `RebinRagged` has been removed.

        self.mantidSnapper.executeQueue()

        # Throughout SNAPRed, the assumption is made that the workspace indices of workspace spectra
        #   are in order of their subgroup-IDs.  This correspondence is validated here.
        # TODO: FIX THIS ISSUE!
        outputWS = self.mantidSnapper.mtd[self.outputWSName]
        for n, subgroupId in enumerate(self.pixelGroup.groupIDs):
            # After `DiffractionFocussing`, the spectrum number for each spectrum is set to its subgroup-ID.
            if outputWS.getSpectrum(n).getSpectrumNo() != subgroupId:
                raise RuntimeError(
                    "Usage error: subgroup IDs for 'PixelGroup' are not in the expected workspace-index order."
                )

        self.setPropertyValue("OutputWorkspace", self.outputWSName)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(FocusSpectraAlgorithm)
