import json
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    FileAction,
    FileProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import (
    Direction,
    StringListValidator,
)

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

logger = snapredLogger.getLogger(__name__)


class FetchGroceriesAlgorithm(PythonAlgorithm):
    """
    For general-purpose loading of nexus data, or grouping workspaces
    """

    def category(self):
        return "SNAPRed data loading"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            FileProperty(
                "Filename",
                defaultValue="",
                action=FileAction.Load,
                extensions=["xml", "h5", "nxs", "hd5"],
                direction=Direction.Input,
            ),
            # propertyMode=PropertyMode.Mandatory,
            doc="Path to file to be loaded",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing the loaded data",
        )
        self.declareProperty(
            "LoaderType",
            "",
            StringListValidator(["", "LoadGroupingDefinition", "LoadNexus", "LoadEventNexus", "LoadNexusProcessed"]),
            direction=Direction.InOut,
        )
        self.declareProperty(
            "InstrumentName",
            defaultValue="",
            direction=Direction.Input,
            doc="Name of an associated instrument",
        )
        self.declareProperty(
            "InstrumentFilename",
            defaultValue="",
            direction=Direction.Input,
            doc="Path of an associated instrument definition file",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("InstrumentDonor", "", Direction.Input, PropertyMode.Optional),
            doc="Workspace to optionally take the instrument from",
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validateInputs(self) -> Dict[str, str]:
        # cannot load a grouping workspace with a nexus loader
        issues: Dict[str, str] = {}
        if self.getProperty("OutputWorkspace").isDefault:
            issues["OutputWorkspace"] = "Must specify the Workspace to load into"
        instrumentSources = [
            self.getPropertyValue("InstrumentName"),
            self.getPropertyValue("InstrumentFilename"),
            self.getPropertyValue("InstrumentDonor"),
        ]
        if sum([1 for s in instrumentSources if s != ""]) > 1:
            issue = "Only one of InstrumentName, InstrumentFilename, or InstrumentDonor can be set"
            issues["InstrumentName"] = issue
            issues["InstrumentFilename"] = issue
            issues["InstrumentDonor"] = issue
        return issues

    def PyExec(self) -> None:
        filename = self.getPropertyValue("Filename")
        outWS = self.getPropertyValue("OutputWorkspace")
        loaderType = self.getPropertyValue("LoaderType")
        # TODO: do we need to guard this with an if?
        if not self.mantidSnapper.mtd.doesExist(outWS):
            if loaderType == "":
                _, loaderType, _ = self.mantidSnapper.Load(
                    "Loading with unspecified loader",
                    Filename=filename,
                    OutputWorkspace=outWS,
                    LoaderName=loaderType,
                )
            elif loaderType == "LoadGroupingDefinition":
                for x in ["InstrumentName", "InstrumentFilename", "InstrumentDonor"]:
                    if not self.getProperty(x).isDefault:
                        instrumentPropertySource = x
                        instrumentSource = self.getPropertyValue(x)
                self.mantidSnapper.LoadGroupingDefinition(
                    "Loading grouping definition",
                    GroupingFilename=filename,
                    OutputWorkspace=outWS,
                    **{instrumentPropertySource: instrumentSource},
                )
            else:
                getattr(self.mantidSnapper, loaderType)(
                    f"Loading data using {loaderType}",
                    Filename=filename,
                    OutputWorkspace=outWS,
                )
        else:
            # TODO: should this throw a warning?  Or warn in logger?
            logger.warning(f"A workspace with name {outWS} already exists in the ADS, and so will not be loaded")
            loaderType = ""
        self.mantidSnapper.executeQueue()
        self.setPropertyValue("LoaderType", str(loaderType))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(FetchGroceriesAlgorithm)
