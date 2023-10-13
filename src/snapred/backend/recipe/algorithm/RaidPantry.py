import json
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
)
from mantid.kernel import (
    Direction,
    FileAction,
    FileProperty,
    StringListValidator,
)

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "RaidPantry"


class RaidPantry(PythonAlgorithm):
    """
    For general-purpose loading scattering data into a workspace.
    """

    def PyInit(self):
        # declare properties
        self.declareProperty("RunNumber", defaultValue=0, direction=Direction.Input)
        self.declareProperty(
            FileProperty(
                "Filename",
                defaultValue="",
                action=FileAction.OptionalLoad,
                extensions=["xml", "h5", "nxs", "hd5"],
            ),
            direction=Direction.Input,
        )
        self.declareProperty(
            "Workspace",
            defaultValue="",
            direction=Direction.Output,
        )
        self.declareProperty(
            "LoaderType",
            "",
            StringListValidator(["LoadNexus", "LoadEventNexus", "LoadNexuxProcessed"]),
            Direction.InOut,
        )
        self.mantidSnapper = MantidSnapper(self, name)

    def alreadyLoaded(self, run: int) -> bool:
        for name, ws in self.mantidSnapper.mtd.items():
            if run == int(ws.getComment()):
                return True
        return False

    def PyExec(self) -> None:
        runNumber = self.getProperty("RunNumber").value
        if self.alreadyLoaded(runNumber):
            return
        filename = self.getProperty("Filename").value
        outWS = self.getProperty("Workspace").value
        loaderType = self.getProperty("LoaderType").value
        if not self.mantidSnapper.mtd.doesExist(outWS):
            if loaderType == "":
                ws = loader = self.mantidSnapper.Load(
                    Filename=filename,
                    OutputWorkspace=outWS,
                )
                self.setPropertyValue("LoaderType", loader)
            else:
                ws = getattr(self.mantidSnapper, loaderType)(
                    Filename=filename,
                    OutputWorkspace=outWS,
                )
        self.mantidSnapper.executeQueue()
        ws.setComment(str(runNumber))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(RaidPantry)
