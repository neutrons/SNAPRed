import json

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    IEventWorkspace,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceUnitValidator,
)
from mantid.kernel import Direction

from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class CreateArtificialNormalizationAlgo(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        self.declareProperty(
            MatrixWorkspaceProperty(
                "InputWorkspace",
                "",
                Direction.Input,
                PropertyMode.Mandatory,
            ),
            doc="Workspace that contains calibrated and focused diffraction data.",
        )
        self.declareProperty(
            MatrixWorkspaceProperty(
                "OutputWorkspace",
                "",
                Direction.Output,
                PropertyMode.Mandatory,
                validator=WorkspaceUnitValidator("dSpacing"),
            ),
            doc="Workspace that contains artificial normalization.",
        )
        self.declareProperty(
            "Ingredients",
            defaultValue="",
            direction=Direction.Input,
        )
        self.declareProperty(
            "ApplyNormalizationIngredients",
            defaultValue="",
            direction=Direction.Input,
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopInredients(self, ingredientsStr: str, applyNormalizationIngredientsStr: str):
        ingredientsDict = json.loads(ingredientsStr)
        self.peakWindowClippingSize = ingredientsDict["peakWindowClippingSize"]
        self.smoothingParameter = ingredientsDict["smoothingParameter"]
        self.decreaseParameter = ingredientsDict["decreaseParameter"]
        self.LSS = ingredientsDict["lss"]

        if applyNormalizationIngredientsStr.strip():
            # Deserialize ApplyNormalizationIngredients into a PixelGroup object
            applyNormalizationIngredients = json.loads(applyNormalizationIngredientsStr)
            self.pixelGroup = PixelGroup(**applyNormalizationIngredients["pixelGroup"])

            # Extract and validate dMin and dMax
            lowdSpacingCrop = Config["constants.CropFactors.lowdSpacingCrop"]
            highdSpacingCrop = Config["constants.CropFactors.highdSpacingCrop"]

            if lowdSpacingCrop < 0:
                raise ValueError("Low d-spacing crop factor must be positive.")
            if highdSpacingCrop < 0:
                raise ValueError("High d-spacing crop factor must be positive.")

            # Compute dMin and dMax
            self.dMin = [x + lowdSpacingCrop for x in self.pixelGroup.dMin()]
            self.dMax = [x - highdSpacingCrop for x in self.pixelGroup.dMax()]

            if not all(dMax > dMin for dMin, dMax in zip(self.dMin, self.dMax)):
                raise ValueError(
                    "d-spacing crop factors are too large -- resultant dMax must be > resultant dMin for all groups."
                )

            # Extract dBin
            self.dBin = self.pixelGroup.dBin()
            if not self.dBin:
                raise ValueError("Calculated dBin is empty or invalid.")

            # Debug logs
            logger.debug(f"Processed Ingredients: dMin={self.dMin}, dMax={self.dMax}, dBin={self.dBin}")
        else:
            # Skip optional steps if ApplyNormalizationIngredients is not provided
            logger.info("ApplyNormalizationIngredients not provided. Skipping optional steps.")
            self.pixelGroup = None
            self.dMin = None
            self.dMax = None
            self.dBin = None

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

    def peakClip(self, data, winSize: int, decrese: bool, LLS: bool, smoothing: float):
        # Clipping peaks from the data with optional smoothing and transformations
        startData = np.copy(data)
        window = winSize
        if smoothing > 0:
            data = self.smooth(data, smoothing)

        if LLS:
            data = self.LLSTransformation(data)

        temp = data.copy()
        scan = list(range(window + 1, 0, -1)) if decrese else list(range(1, window + 1))

        for w in scan:
            for i in range(len(temp)):
                if i < w or i > (len(temp) - w - 1):
                    continue
                winArray = temp[i - w : i + w + 1].copy()
                winArrayReversed = winArray[::-1]
                average = (winArray + winArrayReversed) / 2
                temp[i] = np.min(average[: int(len(average) / 2)])

        if LLS:
            temp = self.InvLLSTransformation(temp)

        index = np.where((startData - temp) == min(startData - temp))[0][0]
        output = temp * (startData[index] / temp[index])
        return output

    def smooth(self, data, order):
        # Applies smoothing to the data
        sm = np.zeros(len(data))
        factor = order / 2 + 1
        for i in range(len(data)):
            temp = 0
            ave = 0
            for r in range(max(0, i - int(order / 2)), min(i + int(order / 2), len(data) - 1) + 1):
                temp += (factor - abs(r - i)) * data[r]
                ave += factor - abs(r - i)
            sm[i] = temp / ave
        return sm

    def LLSTransformation(self, input):  # noqa: A002
        # Applies LLS transformation to emphasize weaker peaks
        return np.log(np.log((input + 1) ** 0.5 + 1) + 1)

    def InvLLSTransformation(self, input):  # noqa: A002
        # Reverts LLS transformation
        return (np.exp(np.exp(input) - 1) - 1) ** 2 - 1

    def PyExec(self):
        self.unbagGroceries()
        ingredients = self.getProperty("Ingredients").value
        applyNormalizationIngredients = self.getProperty("ApplyNormalizationIngredients").value
        self.chopInredients(ingredients, applyNormalizationIngredients)
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace...",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )
        if isinstance(self.mantidSnapper.mtd[self.inputWorkspaceName], IEventWorkspace):
            self.mantidSnapper.ConvertToMatrixWorkspace(
                "Converting event workspace to histogram...",
                InputWorkspace=self.outputWorkspaceName,
                OutputWorkspace=self.outputWorkspaceName,
            )

        self.mantidSnapper.ConvertUnits(
            "Converting to dSpacing...",
            InputWorkspace=self.outputWorkspaceName,
            Target="dSpacing",
            OutputWorkspace=self.outputWorkspaceName,
        )
        if self.pixelGroup and self.dMin and self.dMax:
            self.mantidSnapper.RebinRagged(
                "Resampling X-axis...",
                InputWorkspace=self.outputWorkspaceName,
                XMin=self.dMin,
                XMax=self.dMax,
                Delta=self.pixelGroup.dBin(),
                OutputWorkspace=self.outputWorkspaceName,
                PreserveEvents=False,
            )

        self.mantidSnapper.executeQueue()
        self.inputWorkspace = self.mantidSnapper.mtd[self.inputWorkspaceName]
        self.outputWorkspace = self.mantidSnapper.mtd[self.outputWorkspaceName]

        for i in range(self.outputWorkspace.getNumberHistograms()):
            dataY = self.outputWorkspace.readY(i)
            clippedData = self.peakClip(
                data=dataY,
                winSize=self.peakWindowClippingSize,
                decrese=self.decreaseParameter,
                LLS=self.LSS,
                smoothing=self.smoothingParameter,
            )
            self.outputWorkspace.setY(i, clippedData)

        self.setProperty("OutputWorkspace", self.outputWorkspaceName)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CreateArtificialNormalizationAlgo)
