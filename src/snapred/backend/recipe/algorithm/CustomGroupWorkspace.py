from typing import Dict, List

from mantid.api import AlgorithmFactory, PropertyMode, PythonAlgorithm, WorkspaceGroupProperty
from mantid.kernel import Direction, StringArrayProperty
from mantid.simpleapi import GroupWorkspaces, mtd


class CustomGroupWorkspace(PythonAlgorithm):
    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            StringArrayProperty(
                "GroupingWorkspaces", values=[], direction=Direction.Input, optional=PropertyMode.Mandatory
            )
        )
        self.declareProperty(
            WorkspaceGroupProperty("OutputWorkspace", direction=Direction.Output, optional=PropertyMode.Optional),
            doc="The group of grouping workspaces",
        )
        self.declareProperty("FocusGroups", defaultValue="", direction=Direction.Input)

    def validateInputs(self) -> Dict[str, str]:
        allInputs = list(self.getPropertyValue("GroupingWorkspaces"))
        bads = [x for x in allInputs if not mtd.doesExist(x)]
        errors = {}
        if len(bads) > 0:
            errors["GroupingWorkspaces"] = f"Invalid workspaces: {bads}"
        return errors

    def PyExec(self):
        """
        This algorithm used to orchestrate loading GroupingWorkspaces, then grouping them into a GroupedWorkspace.
        The loading behavior is no longer needed, so removed.  However, some processes seem to anticipate a
        GroupedWorkspaces of the focus GroupingWorkspaces.
        This algo is retained, only to call mantid's GroupWorkspaces, and left only to maintain conformity
        with what used to be there.
        It should be removed, as soon as the GroupedWorkspace functionality is covered.
        """
        # create a workspace group of GroupWorkspaces
        GroupWorkspaces(
            "Grouping workspaces...",
            InputWorkspaces=self.getPropertyValue("GroupingWorkspaces"),
            OutputWorkspace=self.getPropertyValue("OutputWorkspace"),
        )
        # redundant line for redundancy
        self.setPropertyValue("OutputWorkspace", self.getPropertyValue("OutputWorkspace"))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CustomGroupWorkspace)
