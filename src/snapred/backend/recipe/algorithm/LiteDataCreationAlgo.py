from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import LiteDataCreationIngredients
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
            doc="Grouping workspace which maps full pixel resolution to lite data resolution",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Mandatory),
            doc="the workspace reduced to lite resolution and compressed",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)

        # output float property for the tolerance used
        self.declareProperty(
            "Tolerance",
            defaultValue=0.0,
            direction=Direction.Output,
        )

        self.declareProperty(
            "LiteInstrumentDefinitionFile",
            "instrument.lite.definition.file",
            Direction.Input,
        )

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    @staticmethod
    def _appendErrorMessage(errors: Dict[str, str], key: str, msg: str):
        existingMsg = errors.get(key, "")
        if bool(existingMsg):
            existingMsg += "\n"
        errors[key] = existingMsg + msg

    def validateInputs(self) -> Dict[str, str]:
        errors = {}

        if self.getProperty("Ingredients").isDefault:
            errors["Ingredients"] = "A valid instance of 'LiteDataCreationIngredients' must be specified."

        # Make sure that the input workspace is consistent with the lite-data map.
        inputWs = self.mantidSnapper.mtd[self.getPropertyValue("InputWorkspace")]
        groupingWs = self.mantidSnapper.mtd[self.getPropertyValue("LiteDataMapWorkspace")]
        if inputWs.getNumberHistograms() == len(groupingWs.getGroupIDs()):
            # This is definitely a usage error: do not _mask_ defects!
            self._appendErrorMessage(
                errors, "InputWorkspace", "Usage error: the input workspace has already been converted to lite mode."
            )

        else:
            # Implementation notes:
            #
            # In support of generalized lite-mode conversions we require:
            #
            #   a) The number of lite-mode pixels corresponds to the number of subgroups
            #     in the lite-data map (the default pixel-number is |native-mode pixels| / 64),
            #     this must also be the number of pixels in the output instrument.
            #
            #   b) The input-data and lite-mode data map must use the same instrument.
            #
            #   c) The input-data has one spectrum per pixel. In order to include the effect of all
            #     of the input data, this requirement cannot be modified, but might be relaxed
            #     in special cases.
            #
            #   d) Apart from the previous, there is no required relationship between the number of
            #     input spectra, and the number of spectra in the lite-data map, except that the latter
            #     have >= the number of spectra as lite-data pixels.  The "application.yml"
            #     constants will be used to validate these numbers.
            #

            # "a)": Note that we do not have the lite-mode instrument yet, so we don't validate that here,
            #       otherwise we'd need to load the lite-mode instrument twice.
            if len(groupingWs.getGroupIDs()) != Config["instrument.lite.pixelResolution"]:
                self._appendErrorMessage(
                    errors,
                    "LiteDataMapWorkspace",
                    "The lite-data map must have one subgroup for each lite-mode pixel."
                    "\n  The expected lite-mode pixel count may be specified in the 'application.yml'.",
                )

            # "b)": The input-data and lite-data map must have the same instrument.
            # Comparing instruments is problematic: however, requiring both the instrument-name,
            #   and the pixel count to match is a fairly safe verification.
            if (inputWs.getInstrument().getName() != groupingWs.getInstrument().getName()) or (
                inputWs.getInstrument().getNumberDetectors(True) != groupingWs.getInstrument().getNumberDetectors(True)
            ):
                msg = "The lite-data map must have the same instrument as the input data workspace."
                self._appendErrorMessage(errors, "InputWorkspace", msg)
                self._appendErrorMessage(errors, "LiteDataMapWorkspace", msg)

            # "c), d)": the input-data has one spectrum per [non-monitor] pixel
            if (inputWs.getNumberHistograms() != inputWs.getInstrument().getNumberDetectors(True)) or (
                inputWs.getNumberHistograms() != Config["instrument.native.pixelResolution"]
            ):
                self._appendErrorMessage(
                    errors,
                    "InputWorkspace",
                    "The input-data workspace must have one spectrum per non-monitor pixel "
                    + "in the native-mode instrument."
                    + "\n  The expected native-mode pixel count may be specified in the 'application.yml'.",
                )

        self._liteModeResolution = len(groupingWs.getGroupIDs())
        return errors

    def chopIngredients(self, ingredients: LiteDataCreationIngredients):
        instrumentState = ingredients.instrumentState
        self.deltaTOverT = instrumentState.instrumentConfig.delTOverT
        self.delLOverL = instrumentState.instrumentConfig.delLOverL
        self.L = instrumentState.instrumentConfig.L1 + instrumentState.instrumentConfig.L2
        self.delL = self.delLOverL * self.L
        self.deltaTheta = instrumentState.deltaTheta

    def PyExec(self):
        ingredients = LiteDataCreationIngredients.model_validate_json(self.getProperty("Ingredients").value)

        if self.getProperty("LiteInstrumentDefinitionFile").isDefault:
            liteInstrumentDefinitionFileKey = self.getPropertyValue("LiteInstrumentDefinitionFile")
            self.setPropertyValue("LiteInstrumentDefinitionFile", Config[liteInstrumentDefinitionFileKey])

        useCompression = ingredients.instrumentState is not None or ingredients.toleranceOverride is not None
        overrideTolerance = ingredients.toleranceOverride is not None

        # Only calculate the deltaT if an override hasn't been specified.
        if useCompression and not overrideTolerance:
            self.chopIngredients(ingredients)

        # load input workspace
        inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        outputWorkspaceName = self.getPropertyValue("OutputWorkspace")
        groupingWorkspaceName = self.getPropertyValue("LiteDataMapWorkspace")

        # Use `GroupDetectors` algorithm with the specified grouping file to create lite data.
        self.mantidSnapper.GroupDetectors(
            "Creating lite version...",
            InputWorkspace=inputWorkspaceName,
            OutputWorkspace=outputWorkspaceName,
            CopyGroupingFromWorkspace=groupingWorkspaceName,
        )

        if useCompression and not overrideTolerance:
            # Estimate resolution for the unfocused workspace
            resolutionWorkspaceName = f"{outputWorkspaceName}_resolution"
            partialResolutionWorkspaceName = f"{resolutionWorkspaceName}_partial"
            resolutionWorkspaceName = self.mantidSnapper.CloneWorkspace(
                "Copying output workspace for resolution calculation",
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
                SourceDeltaTheta=self.deltaTheta,
            )

            self.mantidSnapper.WashDishes(
                "Remove the partial resolution workspace",
                Workspace=partialResolutionWorkspaceName,
            )

        self.mantidSnapper.executeQueue()

        deltaT = 0.0
        if useCompression and not overrideTolerance:
            # Calculate the resolved 'deltaT' as the negative of the minimum deltaDOverD.
            resolutionWS = self.mantidSnapper.mtd[resolutionWorkspaceName]
            deltaDOverD = resolutionWS.extractY().flatten()
            deltaT = -min(deltaDOverD)
        elif overrideTolerance:
            deltaT = ingredients.toleranceOverride

        self.setProperty("Tolerance", deltaT)

        # replace instrument definition with lite
        self.mantidSnapper.LoadInstrument(
            "Replacing instrument definition with Lite instrument",
            Workspace=outputWorkspaceName,
            Filename=self.getPropertyValue("LiteInstrumentDefinitionFile"),
            RewriteSpectraMap=False,
        )

        if useCompression:
            compressEventsKwargs = {
                "InputWorkspace": outputWorkspaceName,
                "OutputWorkspace": outputWorkspaceName,
            }
            if Config["constants.LiteDataCreationAlgo.toggleCompressionTolerance"]:
                compressEventsKwargs["Tolerance"] = deltaT

            self.mantidSnapper.CompressEvents(
                f"Compressing events in {outputWorkspaceName}...",
                **compressEventsKwargs,
            )

        self.mantidSnapper.executeQueue()

        outputWs = self.mantidSnapper.mtd[outputWorkspaceName]

        # Double-check that the newly-inserted output instrument makes sense:
        #   for efficiency reasons this was not done at `validateInputs`.
        if outputWs.getInstrument().getNumberDetectors(True) != Config["instrument.lite.pixelResolution"]:
            raise RuntimeError("The specified lite-mode instrument does not have the expeced number of pixels")

        # Update the spectrum-to-detector map:
        #   iterate over all of the spectra in the output workspace,
        #   replacing each group of native-mode pixels with a single pixel.
        nHst = outputWs.getNumberHistograms()
        for i in range(nHst):
            el = outputWs.getSpectrum(i)
            el.clearDetectorIDs()
            el.addDetectorID(i)

        # Mark the output workspace as containing lite-mode data.
        # TODO: use metadata for this -- why use the comment?
        outputWs.setComment(outputWs.getComment() + "\nLite")

        # Enforce consistency between the parametrized detector positions,
        #   and those specified in the workspace logs.
        outputWs.populateInstrumentParameters()

        self.setProperty("OutputWorkspace", outputWs)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(LiteDataCreationAlgo)
