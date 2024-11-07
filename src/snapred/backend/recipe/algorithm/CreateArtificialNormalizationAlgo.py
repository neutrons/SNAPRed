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

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

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
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopInredients(self, ingredientsStr: str):
        ingredientsDict = json.loads(ingredientsStr)
        self.peakWindowClippingSize = ingredientsDict["peakWindowClippingSize"]
        self.smoothingParameter = ingredientsDict["smoothingParameter"]
        self.decreaseParameter = ingredientsDict["decreaseParameter"]
        self.LSS = ingredientsDict["lss"]

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
        # Main execution method for the algorithm
        self.unbagGroceries()
        ingredients = self.getProperty("Ingredients").value
        self.chopInredients(ingredients)
        self.mantidSnapper.CloneWorkspace(
            "Cloning input workspace...",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )
        # if input workspace is an eventworkspace, convert it to a histogram workspace
        if isinstance(self.mantidSnapper.mtd[self.inputWorkspaceName], IEventWorkspace):
            # convert it to a histogram
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

        self.mantidSnapper.executeQueue()
        self.inputWorkspace = self.mantidSnapper.mtd[self.inputWorkspaceName]
        self.outputWorkspace = self.mantidSnapper.mtd[self.outputWorkspaceName]

        # Apply peak clipping to each histogram in the workspace
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

        # Set the output workspace property
        self.setProperty("OutputWorkspace", self.outputWorkspaceName)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CreateArtificialNormalizationAlgo)
