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

        # Step 2: Check for overlapping spectra and manage them
        fitPeaksWorkspace = self.mantidSnapper.mtd[self.fitPeaksDiagnosticWorkSpaceName]
        numHistograms = fitPeaksWorkspace.getNumberHistograms()
        processedSpectra = []
        spectrumDict = {}

        for i in range(numHistograms):
            spectrumId = fitPeaksWorkspace.getSpectrum(i).getSpectrumNo()
            singleSpectrumName = f"{self.fitPeaksDiagnosticWorkSpaceName}_spectrum_{spectrumId}"

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
                    InputWorkspace=self.fitPeaksDiagnosticWorkSpaceName,
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
                f"Combining spectrum {spectrum}...", InputWorkspace1=combinedWorkspace, InputWorkspace2=spectrum
            )

        # Step 4: Calculate the residual difference between the combined workspace and input workspace
        self.mantidSnapper.Minus(
            f"Subtracting {combinedWorkspace} from {self.inputWorkspaceName}...",
            LHSWorkspace=combinedWorkspace,
            RHSWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )

        # Execute all queued operations
        self.mantidSnapper.executeQueue()

        # Set the output property to the final residual workspace
        outputWorkspace = self.mantidSnapper.mtd[self.outputWorkspaceName]
        self.setProperty("OutputWorkspace", outputWorkspace)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalculateResidualDiffCalAlgo)
