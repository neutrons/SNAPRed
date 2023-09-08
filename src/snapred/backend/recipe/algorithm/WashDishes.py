import json
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction, StringArrayProperty
from mantid.simpleapi import DeleteWorkspace, DeleteWorkspaces

from snapred.meta.Config import Config

name = "CalculateOffsetDIFC"


class WashDishes(PythonAlgorithm):
    """
    You have made a big mess.  You need to wash up.
    But not unless the CIS wants to investigate the mess first.
    """

    def PyInit(self):
        # declare properties
        self.declareProperty("Workspace", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty(StringArrayProperty(name="WorkspaceList", values=[], direction=Direction.Input))
        self.setRethrows(True)
        self._CISmode: bool = Config["cis_mode"]

    def PyExec(self) -> None:
        self.log().notice("Washing the dishes...")

        workspace = self.getProperty("Workspace")
        if workspace != "" and not self._CISmode:
            DeleteWorkspace(workspace)
        workspaces = self.getProperty("WorkspaceList")
        if workspaces != [] and not self._CISmode:
            workspaces = [w for w in workspaces if isinstance(w, str)]
            DeleteWorkspaces(workspaces)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(WashDishes)
