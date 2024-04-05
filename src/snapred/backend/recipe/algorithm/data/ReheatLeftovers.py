import glob
import json
import os
import tarfile
import tempfile
import time
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    FileAction,
    FileProperty,
    MatrixWorkspace,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction, StringArrayProperty
from mantid.simpleapi import CloneWorkspace, mtd

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


class ReheatLeftovers(PythonAlgorithm):
    """
    Load ragged workspaces with a small number of histograms (< 20?) from a file.
    """

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            FileProperty(
                "Filename",
                defaultValue="",
                action=FileAction.Load,
                extensions=["tar"],
                direction=Direction.Input,
            ),
            doc="Path to file to be loaded",
        )
        self.declareProperty(
            "OutputWorkspace", defaultValue="", direction=Direction.Output, doc="Workspace to be loaded"
        )
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validate(self):
        pass

    def unbagGroceries(self):
        self.outputWSName = self.getPropertyValue("OutputWorkspace")
        self.filename = self.getPropertyValue("Filename")

    def PyExec(self) -> None:
        self.unbagGroceries()
        self.validate()

        with tempfile.TemporaryDirectory(prefix="/tmp/") as extractPath:
            with tarfile.open(self.filename, "r") as tar:
                tar.extractall(path=extractPath, filter="data")

            # collected all files in extractPath
            files = glob.glob(f"{extractPath}/*")
            files = [f for f in files if f.endswith(".nxs")]
            files.sort()
            ws = None
            for file in files:
                index = int(file.split("/")[-1].split(".")[0])
                if ws is None:
                    self.mantidSnapper.LoadNexus(
                        f"Loading Spectra {index}", Filename=file, OutputWorkspace=self.outputWSName
                    )
                    ws = self.outputWSName
                else:
                    tmp = f"{self.outputWSName}_{index}"
                    self.mantidSnapper.LoadNexus(f"Loading Spectra {index}", Filename=file, OutputWorkspace=tmp)
                    self.mantidSnapper.ConjoinWorkspaces(
                        f"Conjoining Spectra {index}", InputWorkspace1=ws, InputWorkspace2=tmp, CheckOverlapping=False
                    )
                self.mantidSnapper.executeQueue()
                wsInst = self.mantidSnapper.mtd[self.outputWSName]
                spec = wsInst.getSpectrum(index)
                spec.setSpectrumNo(index + 1)

        self.setProperty("OutputWorkspace", self.outputWSName)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReheatLeftovers)
