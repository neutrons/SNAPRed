import time

from mantid.api import (
    AlgorithmFactory,
    FileAction,
    FileProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


class WrapLeftovers(PythonAlgorithm):
    """
    Saves ragged workspaces with a small number of histograms (< 20?) from a file.
    """

    NUM_BINS = Config["constants.ResampleX.NumberBins"]
    LOG_BINNING = True

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
                extensions=["nxs.h5"],
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
        # self.tarFilename = self.filename
        # self.filename = self.filename[:-4] + "_{index}.nxs.h5"

    def PyExec(self) -> None:
        self.unbagGroceries()
        self.validate()

        # for index in range(0, self.inputWS.getNumberHistograms()):
        #     # timestamp as name
        #     tmp = str(time.time())
        #     self.mantidSnapper.ResampleX(
        #         "Resampling X-axis...",
        #         InputWorkspace=self.inputWS,
        #         NumberBins=self.NUM_BINS,
        #         LogBinning=self.LOG_BINNING,
        #         OutputWorkspace=tmp,
        #     )

        #     self.mantidSnapper.SaveNexus(
        #         f"Saving Spectra {index}", InputWorkspace=tmp, Filename=self.filename.format(index=index)
        #     )
        #     self.mantidSnapper.executeQueue()

        # timestamp as name
        tmp = str(time.time())
        self.mantidSnapper.ResampleX(
            "Resampling X-axis...",
            InputWorkspace=self.inputWS,
            NumberBins=self.NUM_BINS,
            LogBinning=self.LOG_BINNING,
            OutputWorkspace=tmp,
        )
        self.mantidSnapper.SaveNexus("Saving re-ragged workspace", InputWorkspace=tmp, Filename=self.filename)
        self.mantidSnapper.executeQueue()

        # # finally zip all outputs into a tarball
        # with tarfile.open(self.tarFilename, "w") as tar:
        #     for index in range(0, self.inputWS.getNumberHistograms()):
        #         tar.add(self.filename.format(index=index), arcname=f"{str(index).zfill(2)}.nxs")

        # # clean up
        # for index in range(0, self.inputWS.getNumberHistograms()):
        #     os.remove(self.filename.format(index=index))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(WrapLeftovers)
