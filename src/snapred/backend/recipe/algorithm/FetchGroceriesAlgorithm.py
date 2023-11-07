import json
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import (
    Direction,
    # FileAction,
    # FileProperty,
    StringListValidator,
)

from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class FetchGroceriesAlgorithm(PythonAlgorithm):
    """
    For general-purpose loading scattering data into a workspace.
    """

    def PyInit(self):
        # declare properties
        # self.declareProperty(
        #     FileProperty(
        #         "Filename",
        #         defaultValue="",
        #         action=FileAction.OptionalLoad,
        #         extensions=["xml", "h5", "nxs", "hd5"],
        #     ),
        #     direction=Direction.Input,
        #     propertyMode = PropertyMode.Mandatory,
        #     doc="Path to file to be loaded",
        # )
        self.declareProperty("Filename", "", direction=Direction.Input)
        self.declareProperty(
            MatrixWorkspaceProperty("Workspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing the loaded data",
        )
        self.declareProperty(
            "LoaderType",
            "",
            StringListValidator(["LoadGroupingDefinition", "LoadNexus", "LoadEventNexus", "LoadNexuxProcessed"]),
            Direction.InOut,
        )
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputes(self) -> Dict[str, str]:
        # cannot load a grouping workspace with a nexus loader
        issues: Dict[str, str] = {}
        if self.getProperty("Workspace").isDefault:
            issues["Workspace"] = "Must specify the Workspace to load into"
        # if
        # self.getPropertyValue("IsGroupingWorkspace")
        # and self.getPropertyValue("LoaderType") != "LoadGroupingDefinition":
        #     issue = "Cannot use a nexus loader with a grouping workspace; set to LoadGroupingD"
        #     issues["IsGroupingWorkspace"] = issue
        #     issues["LoaderType"] = issue
        return issues

    def PyExec(self) -> None:
        filename = self.getPropertyValue("Filename")
        outWS = self.getPropertyValue("Workspace")
        loaderType = self.getPropertyValue("LoaderType")
        if not self.mantidSnapper.mtd.doesExist(outWS):
            if loaderType == "":
                self.mantidSnapper.Load(
                    Filename=filename,
                    OutputWorkspace=outWS,
                    LoaderName=loaderType,
                )
            else:
                getattr(self.mantidSnapper, loaderType)(
                    Filename=filename,
                    OutputWorkspace=outWS,
                )
        self.mantidSnapper.executeQueue()
        self.setProperty("LoaderType", self.getPropertyValue(loaderType))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(FetchGroceriesAlgorithm)
