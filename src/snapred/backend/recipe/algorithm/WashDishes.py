import json
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction, StringArrayProperty
from mantid.simpleapi import DeleteWorkspace, DeleteWorkspaces, mtd

from snapred.meta.Config import _Config

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
        Config = _Config()
        self._CISmode: bool = Config["cis_mode"]

    def PyExec(self) -> None:
        self.log().notice("Washing the dishes...")
        workspace = self.getProperty("Workspace").value
        if workspace != "" and not self._CISmode:
            DeleteWorkspace(Workspace=workspace)
        workspaces = self.getProperty("WorkspaceList").value
        if workspaces != [] and not self._CISmode:
            workspaces = [w for w in workspaces if isinstance(w, str)]
            DeleteWorkspaces(workspaces)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(WashDishes)
