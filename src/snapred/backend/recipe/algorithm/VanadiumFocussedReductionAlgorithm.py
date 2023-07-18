import json

from mantid.api import AlgorithmFactory, PythonAlgorithm, WorkspaceGroup, mtd
from mantid.kernel import Direction
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.dao.ReductionIngredients import ReductionIngredients

name = "VanadiumFocussedReductionAlgorithm"

class VanadiumFocussedReductionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        reductionIngredients = ReductionIngredients(**json.loads(self.getProperty("ReductionIngredients").value))
        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        # run the algo
        self.log().notice("Execution of ReductionAlgorithm START!")

        # TODO: Reorg how filepaths are stored
        ipts = reductionIngredients.runConfig.IPTS
        vanadiumFilePath = reductionIngredients.reductionState.stateConfig.vanadiumFilePath
        diffCalPath = reductionIngredients.reductionState.stateConfig.diffractionCalibrant.diffCalPath
        ws_group = WorkspaceGroup()
        mtd.add("vanadiumFocussedReduction", ws_group)

        focussedVanadium = self.mantidSnapper.AlignAndFocusPowder(
            "Diffraction Focussing Vanadium...",
            InputWorkspace=vanadium,
            CalFileName=diffCalPath,
            OutputWorkspace="focussed_vanadium"
        )
        ws_group.add(focussedVanadium)

        peakPositions = ",".join(
            str(s) for s in reductionIngredients.reductionState.stateConfig.normalizationCalibrant.peaks
        )

        smoothedVanadium = self.mantidSnapper.stripPeaks(
            "Stripping Peaks...",
            InputWorkspace=vanadium,
            FWHM=reductionIngredients.reductionState.stateConfig.focusGroups[0].FWHM[0],
            PeakPositions=peakPositions,
            OutputWorkspace="peaks_stripped_vanadium",
        )
        vanadium = self.mantidSnapper.smoothData(
            "Smoothing Data ...",
            InputWorkspace=vanadium,
            NPoints=reductionIngredients.reductionState.stateConfig.normalizationCalibrant.smoothPoints,
            OutputWorkspace="smoothed_data_vanadium",
        )


        # TODO: Refactor so excute only needs to be called once
        self.mantidSnapper.executeQueue()

        groupedData = data
        for workspaceIndex in range(len(focusGroups)):
            data = self.mantidSnapper.rebinRagged(
                "Rebinning ragged bins...",
                InputWorkspace=self.mantidSnapper.mtd[groupedData].getItem(workspaceIndex),
                XMin=focusGroups[workspaceIndex].dMin,
                XMax=focusGroups[workspaceIndex].dMax,
                Delta=focusGroups[workspaceIndex].dBin,
                OutputWorkspace="data_rebinned_ragged_" + str(focusGroups[workspaceIndex].name),
            )
        self.mantidSnapper.deleteWorkspace(Workspace="data_minus_vanadium")
        # self.renameWorkspace(InputWorkspace=data, OutputWorkspace="SomethingSensible")

        self.mantidSnapper.executeQueue()

        if self._export:
            with open("/SNS/users/wqp/git/snapred/snap_reduction.py", "w") as file:
                file.write(self._exportScript)

        self.mantidSnapper.cleanup()
        self.log().notice("Execution of ReductionAlgorithm COMPLETE!")
        return data


# Register algorithm with Mantid
AlgorithmFactory.subscribe(VanadiumFocussedReductionAlgorithm)