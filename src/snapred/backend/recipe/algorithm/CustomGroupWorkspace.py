import json

from mantid.api import AlgorithmFactory, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.state import FocusGroup
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class CustomGroupWorkspace(PythonAlgorithm):
    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty("FocusGroups", defaultValue="", direction=Direction.Input)
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Optional),
            doc="Workspace to take the instrument object from",
        )

        self.declareProperty("InstrumentName", defaultValue="SNAP", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="CommonRed", direction=Direction.Output)
        self.mantidSnapper = MantidSnapper(self, __name__)

    # TODO: This was largely copied from Malcolm's prototype and is due for a refactor
    def PyExec(self):
        focusGroups = parse_raw_as(List[FocusGroup], self.getPropertyValue("FocusGroups"))
        outputWorkspace = self.getPropertyValue("OutputWorkspace")

        donorWorkspace = self.getPropertyValue("InputWorkspace")
        loadEmptyInstrument = bool(not donorWorkspace)
        if loadEmptyInstrument:
            donorWorkspace = "idf"  # name doesn't matter
            # TODO: THIS WILL BREAK
            # I'm not changing it now since LoadGroupingDefinition is replacing this(I think)
            # but this was refactored from LoadInstrument and the params were just carried over
            # These two algos do not share the same params!!!
            self.mantidSnapper.LoadEmptyInstrument(
                "Loading empty instrument...",
                Workspace=donorWorkspace,
                Filename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
                RewriteSpectraMap=False,
            )
            self.mantidSnapper.executeQueue()
        else:
            # convert workspace to string to avoid dangling pointers
            donorWorkspace = str(donorWorkspace)

        for grpIndx, focusGroup in enumerate(focusGroups):
            self.mantidSnapper.LoadGroupingDefinition(
                f"Loading grouping file for focus group {focusGroup.name}...",
                GroupingFilename=focusGroup.definition,
                InstrumentDonor=donorWorkspace,
                OutputWorkspace=focusGroup.name,
            )
            self.mantidSnapper.executeQueue()

        # cleanup temporary workspace
        if loadEmptyInstrument:
            self.mantidSnapper.WashDishes(
                "Deleting empty instrument...",
                Workspace=donorWorkspace,
            )
            self.mantidSnapper.executeQueue()

        # create a workspace group of GroupWorkspaces
        self.mantidSnapper.GroupWorkspaces(
            "Grouping workspaces...",
            InputWorkspaces=[focusGroup.name for focusGroup in focusGroups],
            OutputWorkspace=outputWorkspace,
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CustomGroupWorkspace)
