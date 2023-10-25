import unittest

from mantid.simpleapi import (
    DeleteWorkspace,
    mtd,
)

# the algorithm to test
from snapred.backend.recipe.algorithm.CalculateDiffCalTable import (
    CalculateDiffCalTable as Algo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestCalculateDiffCalTable(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC"""
        self.dBin = 0.001
        self.fakeRawData = "_test_difc_table_raw"
        self.makeFakeNeutronData(self.fakeRawData)

    def tearDown(self) -> None:
        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        return super().tearDown()

    def makeFakeNeutronData(self, rawWsName: str):
        from mantid.simpleapi import (
            CreateSampleWorkspace,
            LoadInstrument,
        )

        # prepare with test data
        CreateSampleWorkspace(
            OutputWorkspace=rawWsName,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=1.2,Sigma=0.2",
            Xmin=0,
            Xmax=5,
            BinWidth=self.dBin,
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )
        LoadInstrument(
            Workspace=rawWsName,
            Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
            RewriteSpectraMap=True,
        )

    def test_init_difc_table(self):
        difcTableWS = "_test_make_difc_table"

        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("CalibrationTable", difcTableWS)
        algo.setProperty("OffsetMode", "Signed")
        algo.setProperty("BinWidth", abs(self.dBin))
        algo.execute()

        difcTable = mtd[difcTableWS]
        for i, row in enumerate(difcTable.column("detid")):
            assert row == i
        for difc in difcTable.column("difc"):
            print(f"{difc},")
