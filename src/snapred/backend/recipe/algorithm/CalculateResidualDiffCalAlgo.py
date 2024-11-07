from typing import Dict

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceGroupProperty,
    WorkspaceUnitValidator,
)
from mantid.kernel import Direction

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

logger = snapredLogger.getLogger(__name__)


class CalculateResidualDiffCalAlgo(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        self.declareProperty(
            MatrixWorkspaceProperty(
                "InputWorkspace",
                "",
                Direction.Input,
                PropertyMode.Mandatory,
                validator=WorkspaceUnitValidator("dSpacing"),
            )
        )
        self.declareProperty(
            MatrixWorkspaceProperty(
                "OutputWorkspace",
                "",
                Direction.Output,
                PropertyMode.Optional,
                validator=WorkspaceUnitValidator("dSpacing"),
            )
        )
        self.declareProperty(
            WorkspaceGroupProperty(
                "FitPeaksDiagnosticWorkSpace",
                "",
                Direction.Input,
                PropertyMode.Mandatory,
            )
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self):
        # NOTE there are no ingredients
        pass

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")
        inputGroupWorkspace = self.getPropertyValue("FitPeaksDiagnosticWorkSpace")

        fitPeaksGroupWorkspace = self.mantidSnapper.mtd[inputGroupWorkspace]
        lastWorkspaceName = fitPeaksGroupWorkspace.getNames()[-1]
        self.fitPeaksDiagnosticWorkSpaceName = lastWorkspaceName

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        return errors

    def PyExec(self):
        self.log().notice("Calculating residual difference for calibration")
        self.unbagGroceries()

        # Step 1: Clone the input workspace to initialize the output workspace
        self.mantidSnapper.CloneWorkspace(
            f"Creating outputWorkspace: {self.outputWorkspaceName}...",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )

        # Step 2: Iterate over each spectrum in fitPeaksDiagnosticWorkSpaceName regardless of its SpectrumNumber
        fitPeaksWorkspace = self.mantidSnapper.mtd[self.fitPeaksDiagnosticWorkSpaceName]
        numHistograms = fitPeaksWorkspace.getNumberHistograms()
        processedSpectra = []

        for i in range(numHistograms):
            # Get the actual SpectrumNumber for each spectrum
            spectrum_id = fitPeaksWorkspace.getSpectrum(i).getSpectrumNo()
            singleSpectrumName = f"{self.fitPeaksDiagnosticWorkSpaceName}_spectrum_{spectrum_id}"

            # Extract each spectrum individually using the index `i`
            self.mantidSnapper.ExtractSingleSpectrum(
                f"Extracting spectrum with SpectrumNumber {spectrum_id}...",
                InputWorkspace=self.fitPeaksDiagnosticWorkSpaceName,
                OutputWorkspace=singleSpectrumName,
                WorkspaceIndex=i,  # Use `i` directly to extract by position
            )

            # Replace zero values with NaN in the extracted spectrum
            self.mantidSnapper.ReplaceSpecialValues(
                f"Replacing zeros with NaN in spectrum with SpectrumNumber {spectrum_id}...",
                InputWorkspace=singleSpectrumName,
                OutputWorkspace=singleSpectrumName,
                SmallNumberThreshold=1e-10,
                SmallNumberValue=np.nan,
            )

            # Append the processed spectrum to the list
            processedSpectra.append(singleSpectrumName)

        # Step 3: Combine all processed spectra into a single workspace
        combinedWorkspace = processedSpectra[0]
        for spectrum in processedSpectra[1:]:
            self.mantidSnapper.ConjoinWorkspaces(
                f"Combining spectrum {spectrum}...", InputWorkspace1=combinedWorkspace, InputWorkspace2=spectrum
            )

        # Step 4: Calculate the residual difference between the combined workspace and input workspace
        self.mantidSnapper.Minus(
            f"Subtracting {combinedWorkspace} from {self.inputWorkspaceName}...",
            LHSWorkspace=combinedWorkspace,
            RHSWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )

        # Clean up temporary spectra workspaces
        self.mantidSnapper.WashDishes(
            "Deleting the workspaces used for processing...",
            Workspace=processedSpectra,
        )

        # Execute all queued operations
        self.mantidSnapper.executeQueue()

        # Set the output property to the final residual workspace
        outputWorkspace = self.mantidSnapper.mtd[self.outputWorkspaceName]
        self.setProperty("OutputWorkspace", outputWorkspace)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalculateResidualDiffCalAlgo)
