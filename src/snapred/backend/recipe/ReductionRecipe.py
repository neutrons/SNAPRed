from typing import Any, Dict, List, Optional, Set, Tuple, Type

from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.ApplyNormalizationRecipe import ApplyNormalizationRecipe
from snapred.backend.recipe.EffectiveInstrumentRecipe import EffectiveInstrumentRecipe
from snapred.backend.recipe.GenerateFocussedVanadiumRecipe import GenerateFocussedVanadiumRecipe
from snapred.backend.recipe.GenericRecipe import ArtificialNormalizationRecipe
from snapred.backend.recipe.PreprocessReductionRecipe import PreprocessReductionRecipe
from snapred.backend.recipe.Recipe import Recipe, WorkspaceName
from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)

Pallet = Tuple[Ingredients, Dict[str, str]]


class ReductionRecipe(Recipe[Ingredients]):
    """
    Currently requires:
        Ingredients:
            pixelGroups: List[PixelGroup]
            detectorPeaksMany: List[List[GroupPeakList]]
            ...
            misc. scalar parameters
            ...


        Groceries:
            # input workspace
            self.sampleWs = groceries["inputWorkspace"]
            # normalization
            self.normalizationWs = groceries.get("normalizationWorkspace", "")
            # combined pixel masks, if any
            self.maskWs = groceries.get("maskWorkspace")
            # list of grouping workspaces
            self.groupingWorkspaces = groceries["groupingWorkspaces"]
    """

    def logger(self):
        return logger

    def mandatoryInputWorkspaces(self) -> Set[WorkspaceName]:
        return {"inputWorkspace", "groupingWorkspaces"}

    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the ingredients.
        """
        self.ingredients = ingredients.copy()
        self.keepUnfocused = self.ingredients.keepUnfocused
        self.convertUnitsTo = self.ingredients.convertUnitsTo

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Unpacks the workspace data from the groceries.
        The input sample data workpsace, inputworkspace, is required, in dspacing

        """
        self.groceries = groceries.copy()
        self.sampleWs = groceries["inputWorkspace"]
        self.normalizationWs = groceries.get("normalizationWorkspace", "")
        self.maskWs = groceries.get("combinedPixelMask", "")
        self.groupingWorkspaces = groceries["groupingWorkspaces"]

    def _cloneWorkspace(self, inputWorkspace: str, outputWorkspace: str) -> str:
        self.mantidSnapper.CloneWorkspace(
            "Cloning workspace...",
            InputWorkspace=inputWorkspace,
            OutputWorkspace=outputWorkspace,
        )
        self.mantidSnapper.executeQueue()
        return outputWorkspace

    def _cloneIntermediateWorkspace(self, inputWorkspace: str, outputWorkspace: str) -> str:
        if self.mantidSnapper.mtd.doesExist(inputWorkspace):
            self.mantidSnapper.MakeDirtyDish(
                "Cloning workspace...", InputWorkspace=inputWorkspace, OutputWorkspace=outputWorkspace
            )
            self.mantidSnapper.executeQueue()
            return inputWorkspace

    def _deleteWorkspace(self, workspace: str):
        self.mantidSnapper.DeleteWorkspace(
            "Deleting workspace...",
            Workspace=workspace,
        )
        self.mantidSnapper.executeQueue()

    def _prepareUnfocusedData(
        self, workspace: WorkspaceName, mask: Optional[WorkspaceName], units: str
    ) -> WorkspaceName:
        unitsAbrev = ""
        match units:
            case "Wavelength":
                unitsAbrev = wng.Units.LAM
            case "MomentumTransfer":
                unitsAbrev = wng.Units.QSP
            case "dSpacing":
                unitsAbrev = wng.Units.DSP
            case "TOF":
                unitsAbrev = wng.Units.TOF
            case _:
                raise ValueError(f"cannot convert to unit '{units}'")

        runNumber, liteMode = workspace.tokens("runNumber", "lite")
        self.unfocWs = wng.run().runNumber(runNumber).lite(liteMode).unit(unitsAbrev).group(wng.Groups.UNFOC).build()
        self._cloneWorkspace(workspace, self.unfocWs)

        if mask:
            self.mantidSnapper.MaskDetectorFlags(
                "Applying pixel mask to unfocused data",
                MaskWorkspace=mask,
                OutputWorkspace=self.unfocWs,
            )

        self.mantidSnapper.ConvertUnits(
            f"Converting unfocused data to {units} units",
            InputWorkspace=self.unfocWs,
            OutputWorkspace=self.unfocWs,
            Target=units,
        )
        self.mantidSnapper.executeQueue()
        return self.unfocWs

    def _prepareArtificialNormalization(self, inputWorkspace: str, groupIndex: int) -> str:
        """
        After the real data has been group processed, we can generate a fake normalization workspace

        :param inputWorkspace: The real data workspace that has been group processed
        :return: The artificial normalization workspace
        """
        normalizationWorkspace = self._getNormalizationWorkspaceName(groupIndex)
        normalizationWorkspace = ArtificialNormalizationRecipe().executeRecipe(
            InputWorkspace=inputWorkspace,
            Ingredients=self.ingredients.artificialNormalizationIngredients,
            OutputWorkspace=normalizationWorkspace,
        )
        self.groceries["normalizationWorkspace"] = normalizationWorkspace
        return normalizationWorkspace

    def _applyRecipe(self, recipe: Type[Recipe], ingredients_, **kwargs):
        if "inputWorkspace" in kwargs:
            inputWorkspace = kwargs["inputWorkspace"]
            if not inputWorkspace:
                self.logger().debug(f"{recipe.__name__} :: Skipping recipe with default empty input workspace")
                return
            if "outputWorkspace" not in kwargs and "outputWorkspace" in self.groceries:
                del self.groceries["outputWorkspace"]
            if self.mantidSnapper.mtd.doesExist(inputWorkspace):
                self.groceries.update(kwargs)
                recipe().cook(ingredients_, self.groceries)
            else:
                raise RuntimeError(
                    (
                        f"{recipe.__name__} ::"
                        " Missing non-default input workspace with groceries:"
                        f" {self.groceries} and kwargs: {kwargs}"
                    )
                )

    def _getNormalizationWorkspaceName(self, groupingIndex: int):
        return f"reduced_normalization_{groupingIndex}_{wnvf.formatTimestamp(self.ingredients.timestamp)}"

    def _prepGroupingWorkspaces(self, groupingIndex: int):
        # TODO:  We need the wng to be able to deconstruct the workspace name
        # so that we can appropriately name the cloned workspaces
        # For now we are just appending it to the end, probably preferable
        # as it keeps the output colocated.
        runNumber, timestamp = self.ingredients.runNumber, self.ingredients.timestamp

        groupingName = self.ingredients.pixelGroups[groupingIndex].focusGroup.name.lower()
        reducedOutputWs = wng.reductionOutput().runNumber(runNumber).group(groupingName).timestamp(timestamp).build()
        sampleClone = self._cloneWorkspace(self.sampleWs, reducedOutputWs)
        self.groceries["inputWorkspace"] = sampleClone
        normalizationClone = None
        if self.normalizationWs:
            normalizationClone = self._cloneWorkspace(
                self.normalizationWs,
                self._getNormalizationWorkspaceName(groupingIndex),
            )
            self.groceries["normalizationWorkspace"] = normalizationClone
        return sampleClone, normalizationClone

    def _isGroupFullyMasked(self, groupingWorkspace: str) -> bool:
        maskWorkspace = self.mantidSnapper.mtd[self.maskWs]
        groupWorkspace = self.mantidSnapper.mtd[groupingWorkspace]

        totalMaskedPixels = 0
        totalGroupPixels = 0

        for i in range(groupWorkspace.getNumberHistograms()):
            group_spectra = groupWorkspace.readY(i)
            for spectrumIndex in group_spectra:
                if maskWorkspace.readY(int(spectrumIndex))[0] == 1:
                    totalMaskedPixels += 1
                totalGroupPixels += 1
        return totalMaskedPixels == totalGroupPixels

    def queueAlgos(self):
        pass

    def execute(self):
        data: Dict[str, Any] = {"result": False}

        # Retain unfocused data for comparison.
        if self.keepUnfocused:
            data["unfocusedWS"] = self._prepareUnfocusedData(self.sampleWs, self.maskWs, self.convertUnitsTo)

        # 1. PreprocessReductionRecipe
        outputs = []
        self._applyRecipe(
            PreprocessReductionRecipe,
            self.ingredients.preprocess(),
            inputWorkspace=self.sampleWs,
            **({"maskWorkspace": self.maskWs} if self.maskWs else {}),
        )
        self._cloneIntermediateWorkspace(self.sampleWs, "sample_preprocessed")
        self._applyRecipe(
            PreprocessReductionRecipe,
            self.ingredients.preprocess(),
            inputWorkspace=self.normalizationWs,
            **({"maskWorkspace": self.maskWs} if self.maskWs else {}),
        )
        self._cloneIntermediateWorkspace(self.normalizationWs, "normalization_preprocessed")

        for groupingIndex, groupingWs in enumerate(self.groupingWorkspaces):
            self.groceries["groupingWorkspace"] = groupingWs

            if self.maskWs and self._isGroupFullyMasked(groupingWs):
                # Notify the user of a fully masked group, but continue with the workflow
                self.logger().warning(
                    f"\nAll pixels masked within {groupingWs} schema.\n"
                    + "Skipping all algorithm execution for this group.\n"
                    + "This will affect future reductions."
                )
                continue

            sampleClone, normalizationClone = self._prepGroupingWorkspaces(groupingIndex)

            # 2. ReductionGroupProcessingRecipe
            self._applyRecipe(
                ReductionGroupProcessingRecipe,
                self.ingredients.groupProcessing(groupingIndex),
                inputWorkspace=sampleClone,
            )
            self._cloneIntermediateWorkspace(sampleClone, f"sample_GroupProcessing_{groupingIndex}")
            self._applyRecipe(
                ReductionGroupProcessingRecipe,
                self.ingredients.groupProcessing(groupingIndex),
                inputWorkspace=normalizationClone,
            )
            self._cloneIntermediateWorkspace(normalizationClone, f"normalization_GroupProcessing_{groupingIndex}")

            vanadiumBasisWorkspace = normalizationClone
            # if there was no normalization and the user elected to use artificial normalization
            # generate one given the params and the processed sample data
            if self.ingredients.artificialNormalizationIngredients:
                vanadiumBasisWorkspace = sampleClone
                normalizationClone = self._getNormalizationWorkspaceName(groupingIndex)

            # 3. GenerateFocussedVanadiumRecipe
            self._applyRecipe(
                GenerateFocussedVanadiumRecipe,
                self.ingredients.generateFocussedVanadium(groupingIndex),
                inputWorkspace=vanadiumBasisWorkspace,
                outputWorkspace=normalizationClone,
            )

            self._cloneIntermediateWorkspace(normalizationClone, f"normalization_FoocussedVanadium_{groupingIndex}")

            # 4. ApplyNormalizationRecipe
            self._applyRecipe(
                ApplyNormalizationRecipe,
                self.ingredients.applyNormalization(groupingIndex),
                inputWorkspace=sampleClone,
                normalizationWorkspace=normalizationClone,
            )
            self._cloneIntermediateWorkspace(sampleClone, f"sample_ApplyNormalization_{groupingIndex}")

            # 5. Replace the instrument with the effective instrument for this grouping
            if Config["reduction.output.useEffectiveInstrument"]:
                self._applyRecipe(
                    EffectiveInstrumentRecipe,
                    self.ingredients.effectiveInstrument(groupingIndex),
                    inputWorkspace=sampleClone,
                )

            # Cleanup
            outputs.append(sampleClone)

            if self.normalizationWs:
                self._deleteWorkspace(normalizationClone)

        if self.maskWs:
            outputs.append(self.maskWs)

        data["result"] = True
        data["outputs"] = outputs
        return data

    def cook(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        """
        Main interface method for the recipe.
        Given the ingredients and groceries, it prepares, executes and returns the final workspace.
        """
        self.prep(ingredients, groceries)
        return self.execute()

    def cater(self, shipment: List[Pallet]) -> List[Dict[str, Any]]:
        """
        A secondary interface method for the recipe.
        It is a batched version of cook.
        Given a shipment of ingredients and groceries, it prepares, executes and returns the final workspaces.
        """
        output = []
        for ingredient, grocery in shipment:
            output.append(self.cook(ingredient, grocery))
        return output
