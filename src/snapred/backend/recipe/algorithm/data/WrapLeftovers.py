import json
import os
import tarfile
import time
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
from mantid.kernel import Direction, StringArrayProperty
from mantid.simpleapi import CloneWorkspace, mtd
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


class WrapLeftovers(PythonAlgorithm):
    """
    Saves ragged workspaces with a small number of histograms (< 20?) from a file.
    """

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace to be saved.",
        )
        self.declareProperty(
            FileProperty(
                "Filename",
                defaultValue="",
                action=FileAction.Save,
                extensions=["tar"],
                direction=Direction.Input,
            ),
            doc="Path to file to be loaded",
        )

        self.mantidSnapper = MantidSnapper(self, __name__)

    def validate(self):
        numHisto = self.inputWS.getNumberHistograms()
        if numHisto > 30:
            raise ValueError(f"Too many histograms to save, this isnt the write tool for the job!: {numHisto}")

    def unbagGroceries(self):
        self.inputWS = self.mantidSnapper.mtd[self.getPropertyValue("InputWorkspace")]
        self.filename = self.getPropertyValue("Filename")
        self.tarFilename = self.filename
        self.filename = self.filename[:-4] + "_{index}.nxs"

    def PyExec(self) -> None:
        self.unbagGroceries()
        self.validate()

        for index in range(0, self.inputWS.getNumberHistograms()):
            # timestamp as name
            tmp = str(time.time())
            self.mantidSnapper.ExtractSpectra(
                f"Extracting Spectra {index}",
                InputWorkspace=self.inputWS,
                OutputWorkspace=tmp,
                StartWorkspaceIndex=index,
                EndWorkspaceIndex=index,
            )
            self.mantidSnapper.SaveNexus(
                f"Saving Spectra {index}", InputWorkspace=tmp, Filename=self.filename.format(index=index)
            )
            self.mantidSnapper.DeleteWorkspace(f"Deleting Spectra {index}", Workspace=tmp)
            self.mantidSnapper.executeQueue()

        # finally zip all outputs into a tarball
        with tarfile.open(self.tarFilename, "w") as tar:
            for index in range(0, self.inputWS.getNumberHistograms()):
                tar.add(self.filename.format(index=index), arcname=f"{index}.nxs")

        # clean up
        for index in range(0, self.inputWS.getNumberHistograms()):
            os.remove(self.filename.format(index=index))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(WrapLeftovers)
