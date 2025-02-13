from types import NoneType
from typing import Dict, Set

import numpy as np
from pydantic import BaseModel

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.Recipe import Recipe
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

logger = snapredLogger.getLogger(__name__)


class CalculateDiffCalServing(BaseModel):
    outputWorkspace: str


class CalculateDiffCalResidualRecipe(Recipe[None]):
    def __init__(self, utensils: Utensils = None):
        if utensils is None:
            utensils = Utensils()
            utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper
        self._counts = 0

    def logger(self):
        return logger

    def allGroceryKeys(self) -> Set[str]:
        return {"inputWorkspace", "outputWorkspace", "fitPeaksDiagnosticWorkspace"}

    def mandatoryInputWorkspaces(self) -> Set[str]:
        return {"inputWorkspace", "fitPeaksDiagnosticWorkspace"}

    def chopIngredients(self, ingredients: NoneType = None) -> None:
        """Receive the ingredients from the recipe."""
        pass

    def unbagGroceries(self, groceries: Dict[str, WorkspaceName]):
        self.inputWorkspaceName = groceries["inputWorkspace"]
        self.outputWorkspaceName = groceries["outputWorkspace"]
        diagnosticWSname = groceries["fitPeaksDiagnosticWorkspace"]
        diagnosticWorkspace = self.mantidSnapper.mtd[diagnosticWSname]
        lastWorkspaceName = diagnosticWorkspace.getNames()[-1]
        self.fitPeaksDiagnosticWorkspaceName = lastWorkspaceName

    def prep(self, ingredients: NoneType, groceries: Dict[str, WorkspaceName]):
        """
        Convenience method to prepare the recipe for execution.
        """
        self.validateInputs(ingredients, groceries)
        self.chopIngredients(ingredients)
        self.unbagGroceries(groceries)
        self.stirInputs()
        self.queueAlgos()

    def queueAlgos(self):
        # Step 1: Clone the input workspace to initialize the output workspace
        self.mantidSnapper.CloneWorkspace(
            f"Creating outputWorkspace: {self.outputWorkspaceName}...",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )

        # Step 2: Check for overlapping spectra and manage them
        fitPeaksWorkspace = self.mantidSnapper.mtd[self.fitPeaksDiagnosticWorkspaceName]
        numHistograms = fitPeaksWorkspace.getNumberHistograms()
        processedSpectra = []
        spectrumDict = {}

        for i in range(numHistograms):
            spectrumId = fitPeaksWorkspace.getSpectrum(i).getSpectrumNo()
            singleSpectrumName = f"{self.fitPeaksDiagnosticWorkspaceName}_spectrum_{spectrumId}"

            # If this spectrum number is already processed, average with existing
            if spectrumId in spectrumDict:
                existingName = spectrumDict[spectrumId]
                self.mantidSnapper.Plus(
                    f"Averaging overlapping spectrum {spectrumId}...",
                    LHSWorkspace=existingName,
                    RHSWorkspace=singleSpectrumName,
                    OutputWorkspace=singleSpectrumName,
                )
            else:
                # Extract spectrum by position
                self.mantidSnapper.ExtractSingleSpectrum(
                    f"Extracting spectrum with SpectrumNumber {spectrumId}...",
                    InputWorkspace=self.fitPeaksDiagnosticWorkspaceName,
                    OutputWorkspace=singleSpectrumName,
                    WorkspaceIndex=i,
                )

                # Replace zero values with NaN
                self.mantidSnapper.ReplaceSpecialValues(
                    f"Replacing zeros with NaN in spectrum with SpectrumNumber {spectrumId}...",
                    InputWorkspace=singleSpectrumName,
                    OutputWorkspace=singleSpectrumName,
                    SmallNumberThreshold=1e-10,
                    SmallNumberValue=np.nan,
                )

                spectrumDict[spectrumId] = singleSpectrumName

            # Append the processed spectrum to the list
            processedSpectra.append(singleSpectrumName)

        # Step 3: Combine all processed spectra into a single workspace
        combinedWorkspace = processedSpectra[0]
        for spectrum in processedSpectra[1:]:
            self.mantidSnapper.ConjoinWorkspaces(
                f"Combining spectrum {spectrum}...",
                InputWorkspace1=combinedWorkspace,
                InputWorkspace2=spectrum,
                CheckMatchingBins=False,  # not available in 6.11.0.3rc2
            )

        # Step 4: Calculate the residual difference between the combined workspace and input workspace
        self.mantidSnapper.Minus(
            f"Subtracting {combinedWorkspace} from {self.inputWorkspaceName}...",
            LHSWorkspace=combinedWorkspace,
            RHSWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )

    def execute(self):
        self.mantidSnapper.executeQueue()
        # Set the output property to the final residual workspace
        self.outputWorkspace = self.mantidSnapper.mtd[self.outputWorkspaceName]

    def cook(self, ingredients: NoneType, groceries: Dict[str, WorkspaceName]):  # noqa ARG002
        self.prep(None, groceries)
        self.execute()
        return CalculateDiffCalServing(
            outputWorkspace=self.outputWorkspaceName,
        )
