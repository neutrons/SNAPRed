from typing import Any, Dict, List, Optional, Set, Tuple, Type

from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.ApplyNormalizationRecipe import ApplyNormalizationRecipe
from snapred.backend.recipe.EffectiveInstrumentRecipe import EffectiveInstrumentRecipe
from snapred.backend.recipe.GenerateFocussedVanadiumRecipe import GenerateFocussedVanadiumRecipe
from snapred.backend.recipe.GenericRecipe import ArtificialNormalizationRecipe
from snapred.backend.recipe.Recipe import Recipe, WorkspaceName
from snapred.backend.recipe.ReductionGroupProcessingRecipe import ReductionGroupProcessingRecipe
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

_logger = snapredLogger.getLogger(__name__)

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
        return _logger

    def allGroceryKeys(self) -> Set[str]:
        return {
            "inputWorkspace",
            "groupingWorkspaces",
            "diffcalWorkspace",
            "normalizationWorkspace",
            "combinedPixelMask",
        }

    def mandatoryInputWorkspaces(self) -> Set[str]:
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
        The input sample-data workspace should be in units of d-spacing.

        """
        # Implementation note:
        # `self.groceries` is no longer passed around _implicitly_:
        #    anything needed by a sub-recipe should be passed in explicitly,
        #    and should be unbagged _here_.

        self.sampleWs = groceries["inputWorkspace"]
        self.diffcalWs = groceries.get("diffcalWorkspace", "")
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

    def _addOrReplaceToOutput(self, preOutputWsName: WorkspaceName) -> WorkspaceName:
        nameBuilder = preOutputWsName.builder
        if nameBuilder is None:
            raise RuntimeError(
                "Cannot generate a new name for add-or-replace using an incomplete `WorkspaceName`:\n"
                + f"    '{preOutputWsName}': {type(preOutputWsName)}"
            )
        # Remove the 'hidden' prefix from the original name.
        finalizedOutputWsName = nameBuilder.hidden(False).build()

        self.mantidSnapper.RenameWorkspace(
            "Add-or-replace to finalized output",
            OutputWorkspace=finalizedOutputWsName,
            InputWorkspace=preOutputWsName,
            OverwriteExisting=True,
        )
        self.mantidSnapper.executeQueue()
        return finalizedOutputWsName

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

        # In case of live-data reduction, we need to add-or-replace the unfocused-data workspace in one operation,
        #   as the user may be viewing a plot of this workspace from a previous live-data cycle.
        # For this reason, we start with a hidden workspace, and then rename it at the end
        #   of this method's algorithm sequence.
        preOutputUnfocWs = (
            wng.run().runNumber(runNumber).lite(liteMode).unit(unitsAbrev).group(wng.Groups.UNFOC).hidden(True).build()
        )

        # In order that there be only one algorithm queue here, we do not use `self._cloneWorkspace`.
        self.mantidSnapper.CloneWorkspace(
            "Cloning unfocused data", OutputWorkspace=preOutputUnfocWs, InputWorkspace=workspace
        )
        if mask:
            self.mantidSnapper.MaskDetectorFlags(
                "Applying pixel mask to unfocused data",
                MaskWorkspace=mask,
                OutputWorkspace=preOutputUnfocWs,
            )

        self.mantidSnapper.ConvertUnits(
            f"Converting unfocused data to {units}",
            InputWorkspace=preOutputUnfocWs,
            OutputWorkspace=preOutputUnfocWs,
            Target=units,
        )

        # Rename the workspace
        self.unfocWs = preOutputUnfocWs.builder.hidden(False).build()
        self.mantidSnapper.RenameWorkspace(
            "Add-or-replace unfocused data",
            OutputWorkspace=self.unfocWs,
            InputWorkspace=preOutputUnfocWs,
            OverwriteExisting=True,
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
        return normalizationWorkspace

    def _applyRecipe(self, recipe: Type[Recipe], ingredients_, **kwargs):
        # Implementation notes:
        #   * We no longer pass `self.groceries` around _implicitly_: this was a really bad idea!
        #   * There's not a lot of value added in not letting the recipes deal with their own error messages.
        #     For this reason, we do _not_ check the presence of, or the existence of any arguments _here_.

        recipe().cook(ingredients_, groceries=kwargs)

    def _getNormalizationWorkspaceName(self, groupingIndex: int):
        return f"reduced_normalization_{groupingIndex}_{wnvf.formatTimestamp(self.ingredients.timestamp)}"

    def _generateWorkspaceNamesForGroup(self, groupingIndex: int):
        # TODO:  We need the wng to be able to deconstruct the workspace name
        # so that we can appropriately name the cloned workspaces
        # For now we are just appending it to the end, probably preferable
        # as it keeps the output colocated.
        runNumber, timestamp = self.ingredients.runNumber, self.ingredients.timestamp

        groupingName = self.ingredients.pixelGroups[groupingIndex].focusGroup.name.lower()

        # Reduction of live data requires the reduced output workspace to be updated in _one_ operation,
        #   at the _end_ of the reduction process.  This allows plotting output workspaces in real time.
        # We temporarily prefix the output workspace with the double underscore (i.e. the "hidden" attribute),
        #   which will then be removed during the add-or-replace step (i.e. rename) during finalization.
        reducedOutputWs = (
            wng.reductionOutput().runNumber(runNumber).group(groupingName).timestamp(timestamp).hidden(True).build()
        )

        sampleClone = reducedOutputWs
        normalizationClone = None
        if self.normalizationWs:
            normalizationClone = self._getNormalizationWorkspaceName(groupingIndex)
        return sampleClone, normalizationClone

    def _isGroupFullyMasked(self, groupingIndex: int) -> bool:
        pgps = self.ingredients.pixelGroups[groupingIndex].pixelGroupingParameters
        return len(pgps) == 0

    def _maskedSubgroups(self, groupingIndex: int) -> List[int]:
        pgps = self.ingredients.pixelGroups[groupingIndex].pixelGroupingParameters
        unmaskedPgps = self.ingredients.unmaskedPixelGroups[groupingIndex].pixelGroupingParameters
        subgroups = []
        if len(pgps) != len(unmaskedPgps):
            subgroups = [subgroupId for subgroupId in unmaskedPgps if subgroupId not in pgps]
        return subgroups

    def queueAlgos(self):
        pass

    def execute(self):
        data: Dict[str, Any] = {"result": False}

        # Retain unfocused data for comparison.
        if self.keepUnfocused:
            data["unfocusedWS"] = self._prepareUnfocusedData(self.sampleWs, self.maskWs, self.convertUnitsTo)

        outputs = []

        if bool(self.maskWs) and all(
            (self._isGroupFullyMasked(groupingIndex) for groupingIndex in range(len(self.groupingWorkspaces)))
        ):
            raise RuntimeError(
                "There are no unmasked pixels in any of the groupings.  Please check your mask workspace!"
            )
        self.mantidSnapper.ConvertUnits(
            "Converting sample data to d-spacing",
            InputWorkspace=self.sampleWs,
            OutputWorkspace=self.sampleWs,
            Target="dSpacing",
            EMode="Elastic",
        )

        if self.normalizationWs:
            # Temporarily convert the normalization workspace to d-spacing
            self.mantidSnapper.ConvertUnits(
                "Converting normalization data to d-spacing",
                InputWorkspace=self.normalizationWs,
                OutputWorkspace=self.normalizationWs,
                Target="dSpacing",
                EMode="Elastic",
            )
        self.mantidSnapper.executeQueue()

        for groupingIndex, groupingWs in enumerate(self.groupingWorkspaces):
            if bool(self.maskWs):
                if self._isGroupFullyMasked(groupingIndex):
                    # Notify the user of a fully-masked group, and then skip this grouping.
                    self.logger().warning(
                        f"\nAll pixels within the '{self.ingredients.pixelGroups[groupingIndex].focusGroup.name}' "
                        + "grouping are masked.\n"
                        + "This grouping will be skipped!"
                    )
                    continue
                maskedSubgroups = self._maskedSubgroups(groupingIndex)
                if len(maskedSubgroups) > 0:
                    # Notify the user of any fully-masked subgroups in this grouping.
                    self.logger().warning(
                        f"\nWithin the '{self.ingredients.pixelGroups[groupingIndex].focusGroup.name}' "
                        + f"grouping:\n    subgroups {maskedSubgroups} are fully masked."
                    )

            # NOTE: DONT clone either here, let ReductionGroupProcessingRecipe do it
            sampleClone, normalizationClone = self._generateWorkspaceNamesForGroup(groupingIndex)

            # 2. ReductionGroupProcessingRecipe
            self._applyRecipe(
                # groceries: 'inputWorkspace', 'groupingWorkspace', 'maskWorkspace' [, 'outputWorkspace']
                ReductionGroupProcessingRecipe,
                self.ingredients.groupProcessing(groupingIndex),
                inputWorkspace=self.sampleWs,
                outputWorkspace=sampleClone,
                groupingWorkspace=groupingWs,
                **({"maskWorkspace": self.maskWs} if self.maskWs else {}),
            )
            self._cloneIntermediateWorkspace(sampleClone, f"sample_GroupProcessing_{groupingIndex}")

            if normalizationClone:
                self._applyRecipe(
                    # groceries: 'inputWorkspace', 'groupingWorkspace', 'maskWorkspace' [, 'outputWorkspace']
                    ReductionGroupProcessingRecipe,
                    self.ingredients.groupProcessing(groupingIndex),
                    inputWorkspace=self.normalizationWs,
                    outputWorkspace=normalizationClone,
                    groupingWorkspace=groupingWs,
                    **({"maskWorkspace": self.maskWs} if self.maskWs else {}),
                )
                self._cloneIntermediateWorkspace(normalizationClone, f"normalization_GroupProcessing_{groupingIndex}")

            vanadiumBasisWorkspace = normalizationClone
            # if there was no normalization and the user elected to use artificial normalization
            # generate one given the params and the processed sample data
            if self.ingredients.artificialNormalizationIngredients:
                vanadiumBasisWorkspace = sampleClone
                normalizationClone = self._getNormalizationWorkspaceName(groupingIndex)

            # 3. GenerateFocussedVanadiumRecipe

            ##
            ## TODO: =====> THIS NEXT SUB-RECIPE modifies its 'inputWorkspace': that behavior is not really OK! <======
            ##   This workspace will be either of 'normalizationClone' or 'sampleClone':
            ##     both of these are then used as _input_ for later sub-recipes.
            ##   In the case that the input workspace is `sampleClone` it should possibly be cloned -again- instead.
            ##
            if normalizationClone:
                self._applyRecipe(
                    # groceries: 'inputWorkspace' [, 'outputWorkspace']
                    GenerateFocussedVanadiumRecipe,
                    self.ingredients.generateFocussedVanadium(groupingIndex),
                    inputWorkspace=vanadiumBasisWorkspace,
                    outputWorkspace=normalizationClone,
                )

                self._cloneIntermediateWorkspace(normalizationClone, f"normalization_FoocussedVanadium_{groupingIndex}")

            # 4. ApplyNormalizationRecipe
            self._applyRecipe(
                # groceries: 'inputWorkspace', ['normalizationWorkspace': ''] [, 'backgroundWorkspace': '']
                ApplyNormalizationRecipe,
                self.ingredients.applyNormalization(groupingIndex),
                inputWorkspace=sampleClone,
                normalizationWorkspace=normalizationClone,
            )
            self._cloneIntermediateWorkspace(sampleClone, f"sample_ApplyNormalization_{groupingIndex}")

            # 5. Replace the instrument with the effective instrument for this grouping
            if Config["reduction.output.useEffectiveInstrument"]:
                self._applyRecipe(
                    # groceries: 'inputWorkspace' [, 'outputWorkspace']
                    EffectiveInstrumentRecipe,
                    self.ingredients.effectiveInstrument(groupingIndex),
                    inputWorkspace=sampleClone,
                )

            ## Finalization:

            # Add-or-replace to the actual reduced output workspace (required for reducing live data).
            reducedOutputWs = self._addOrReplaceToOutput(sampleClone)

            # Retain the output workspace in the 'outputs' list.
            outputs.append(reducedOutputWs)

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
