from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


class LiteDataCreationAlgo(PythonAlgorithm):
    def category(self):
        return "SNAPRed Lite mode reduction"

    def PyInit(self):
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing full resolution the data set to be converted to lite",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("LiteDataMapWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Grouping workspace which converts maps full pixel resolution to lite data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Mandatory),
            doc="the workspace reduced to lite resolution and compressed",
        )
        self.declareProperty(
            "Ingredients",
            defaultValue="",
            direction=Direction.Input,
        )
        # TODO this is needed by LoadInstrument, which needs to be removed
        self.declareProperty(
            "LiteInstrumentDefinitionFile",
            str(Config["instrument.lite.definition.file"]),
            Direction.Input,
        )
        self.declareProperty("AutoDeleteNonLiteWS", defaultValue=False, direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        # make sure map is consistent with input data
        inWS = self.mantidSnapper.mtd[self.getPropertyValue("InputWorkspace")]
        groupWS = self.mantidSnapper.mtd[self.getPropertyValue("LiteDataMapWorkspace")]
        if inWS.getNumberHistograms() == len(groupWS.getGroupIDs()):
            # the data has already been reduced -- not an issue
            pass
        elif inWS.getNumberHistograms() != groupWS.getNumberHistograms():
            msg = f"""
                The workspaces {self.getPropertyValue("InputWorkspace")}
                and {self.getPropertyValue("LiteDataMapWorkspace")}
                have inconsistent number of spectra
                """
            errors["InputWorkspace"] = msg
            errors["LiteDataMapWorkspace"] = msg
        self._liteModeResolution = len(groupWS.getGroupIDs())
        return errors

    def chopIngredients(self, ingredients: InstrumentState):
        self.deltaTOverT = ingredients.instrumentConfig.delTOverT
        self.delLOverL = ingredients.instrumentConfig.delLOverL
        self.L = ingredients.instrumentConfig.L1 + ingredients.instrumentConfig.L2
        self.delL = self.delLOverL * self.L
        self.delTheta = ingredients.delTh
        return

    def PyExec(self):
        self.log().notice("Lite Data Creation START!")
        ingredients = InstrumentState.model_validate_json(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        # load input workspace
        inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        outputWorkspaceName = self.getPropertyValue("OutputWorkspace")
        groupingWorkspaceName = self.getPropertyValue("LiteDataMapWorkspace")

        # flag for auto deletion of non-lite data
        autoDelete = self.getProperty("AutoDeleteNonLiteWS").value

        # check that we are not already in lite mode
        # if we are, simply copy over the workspace and continue
        # lite mode could be indicated by:
        # - the word "Lite" in the comments of the workspace
        # - the workspace having exactly as many pixels as Lite data
        inputWS = self.mantidSnapper.mtd[inputWorkspaceName]
        if "Lite" in inputWS.getComment() or inputWS.getNumberHistograms() == self._liteModeResolution:
            self.log().notice("Input data already in Lite mode")
            if outputWorkspaceName != inputWorkspaceName:
                self.mantidSnapper.CloneWorkspace(
                    "Workspace already lite, cloning it to output",
                    InputWorkspace=inputWorkspaceName,
                    OutputWorkspace=outputWorkspaceName,
                )
                self.mantidSnapper.executeQueue()
            # add the word "Lite" to the comments for good measure
            outWS = self.mantidSnapper.mtd[outputWorkspaceName]
            outWS.setComment(outWS.getComment() + "\nLite")
            self.setProperty("OutputWorkspace", outWS)
            return  # NOTE EARLY

        # use group detector with specific grouping file to create lite data
        self.mantidSnapper.GroupDetectors(
            "Creating lite version...",
            InputWorkspace=inputWorkspaceName,
            OutputWorkspace=outputWorkspaceName,
            CopyGroupingFromWorkspace=groupingWorkspaceName,
        )

        # Estimate resolution for the unfocused workspace
        resolutionWorkspaceName = f"{outputWorkspaceName}_resolution"
        partialResolutionWorkspaceName = f"{resolutionWorkspaceName}_partial"
        resolutionWorkspaceName = self.mantidSnapper.CloneWorkspace(
            "Workspace already lite, cloning it to output",
            InputWorkspace=outputWorkspaceName,
            OutputWorkspace=resolutionWorkspaceName,
        )

        self.mantidSnapper.EstimateResolutionDiffraction(
            f"Estimating resolution for {outputWorkspaceName}...",
            InputWorkspace=resolutionWorkspaceName,
            OutputWorkspace=resolutionWorkspaceName,
            PartialResolutionWorkspaces=partialResolutionWorkspaceName,
            DeltaTOFOverTOF=self.deltaTOverT,
            SourceDeltaL=self.delL,
            SourceDeltaTheta=self.delTheta,
        )

        self.mantidSnapper.WashDishes(
            "Remove the partial resolution workspace",
            Workspace=partialResolutionWorkspaceName,
        )

        # Calculate ΔΤ as the negative of the minimum deltaDOverD
        self.mantidSnapper.executeQueue()
        resolutionWS = self.mantidSnapper.mtd[resolutionWorkspaceName]
        deltaDOverD = resolutionWS.extractY().flatten()
        deltaT = -min(deltaDOverD)

        # TODO how can this be removed?
        # replace instrument definition with lite
        self.mantidSnapper.LoadInstrument(
            "Replacing instrument definition with Lite instrument",
            Workspace=outputWorkspaceName,
            Filename=self.getPropertyValue("LiteInstrumentDefinitionFile"),
            RewriteSpectraMap=False,
        )

        # TODO is this necessary?
        self.mantidSnapper.CompressEvents(
            f"Compressing events in {outputWorkspaceName}...",
            InputWorkspace=outputWorkspaceName,
            OutputWorkspace=outputWorkspaceName,
            Tolerance=deltaT,
        )

        if autoDelete is True:
            self.mantidSnapper.WashDishes(
                f"Deleting {inputWorkspaceName}...",
                Workspace=inputWorkspaceName,
            )

        # iterate over spectra in grouped workspace, delete old ids and replace
        self.mantidSnapper.executeQueue()
        liteWksp = self.mantidSnapper.mtd[outputWorkspaceName]
        nHst = liteWksp.getNumberHistograms()
        for i in range(nHst):
            el = liteWksp.getSpectrum(i)
            el.clearDetectorIDs()
            el.addDetectorID(i)
        liteWksp.setComment(liteWksp.getComment() + "\nLite")
        self.setProperty("OutputWorkspace", liteWksp)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LiteDataCreationAlgo)
