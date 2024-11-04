import unittest

from mantid.api import WorkspaceGroup
from mantid.simpleapi import (
    CalculateDiffCalTable,
    ConjoinDiagnosticWorkspaces,
    CreateTableWorkspace,
    CreateWorkspace,
    DiffractionFocussing,
    ExtractSingleSpectrum,
    PDCalibration,
    mtd,
)
from mantid.testing import assert_almost_equal
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import deleteWorkspaceNoThrow

# the algorithm to test
from snapred.backend.recipe.algorithm.ConjoinDiagnosticWorkspaces import (
    ConjoinDiagnosticWorkspaces as Algo,  # noqa: E402
)
from snapred.meta.pointer import create_pointer


class TestConjoinDiagnosticWorkspaces(unittest.TestCase):
    @classmethod
    def tearDownClass(cls) -> None:
        """
        Delete all workspaces created by this test, and remove any created files.
        This is run once at the end of this test suite.
        """
        for ws in mtd.getObjectNames():
            deleteWorkspaceNoThrow(ws)
        return super().tearDownClass()

    def test_naming(self):
        oldNames = ["xyz_d4_dspacing_2", "not_in_list", "feg_abc_fitted_fx"]
        expNames = ["fun_dspacing", "fun_fitted"]
        algo = Algo()
        algo.initialize()
        assert expNames == algo.newNamesFromOld(oldNames, "fun")

    def test_simple_success_table(self):
        # make group 1
        name1 = mtd.unique_name()
        group1 = WorkspaceGroup()
        tab1 = CreateTableWorkspace(
            OutputWorkspace=f"{name1}_fitted",
            Data=create_pointer({"col1": [1, 2], "col3": [4, 5]}),
        )
        group1.addWorkspace(tab1)
        mtd.add(name1, group1)

        # make group 2
        name2 = mtd.unique_name()
        group2 = WorkspaceGroup()
        tab2 = CreateTableWorkspace(
            OutputWorkspace=f"{name2}_fitted",
            Data=create_pointer({"col1": [11, 12], "col2": [3, 6]}),
        )
        group2.addWorkspace(tab2)
        mtd.add(name2, group2)

        finalname = mtd.unique_name()

        # add in the first group
        algo = Algo()
        algo.initialize()
        algo.diagnosticSuffix = {0: "_fitted"}
        algo.setProperty("DiagnosticWorkspace", name1)
        algo.setProperty("TotalDiagnosticWorkspace", finalname)
        algo.setProperty("AddAtIndex", 0)
        algo.execute()

        finalGroup = mtd[algo.getPropertyValue("TotalDiagnosticWorkspace")]
        tabDict = finalGroup.getItem(0).toDict()
        assert tabDict == {"col1": [1, 2], "col3": [4, 5]}

        # add in the second group
        algo = Algo()
        algo.initialize()
        algo.diagnosticSuffix = {0: "_fitted"}
        algo.setProperty("DiagnosticWorkspace", name2)
        algo.setProperty("TotalDiagnosticWorkspace", finalname)
        algo.setProperty("AddAtIndex", 1)
        algo.execute()

        finalGroup = mtd[algo.getPropertyValue("TotalDiagnosticWorkspace")]
        tabDict = finalGroup.getItem(0).toDict()
        assert tabDict == {"col1": [1, 2, 11, 12], "col2": [0, 0, 3, 6], "col3": [4, 5, 0, 0]}

    def test_simple_success_workspace(self):
        bigwksp = CreateWorkspace(
            DataX=[0, 1, 2, 0, 1, 2],
            DataY=[2, 2, 3, 3],
            NSpec=2,
        )
        # group 1
        name1 = mtd.unique_name(prefix="group1_")
        group1 = WorkspaceGroup()
        wksp1 = ExtractSingleSpectrum(
            InputWorkspace=bigwksp,
            OutputWorkspace=f"{name1}_dspacing",
            WorkspaceIndex=0,
        )
        group1.addWorkspace(wksp1)
        mtd.add(name1, group1)

        # group 2
        name2 = mtd.unique_name(prefix="group2_")
        group2 = WorkspaceGroup()
        wksp2 = CreateWorkspace(
            OutputWorkspace=f"{name2}_dspacing",
            DataX=[0, 0, 0, 0, 1, 2],
            DataY=[1000, 1000, 3, 3],
            NSpec=2,
        )
        group2.addWorkspace(wksp2)
        mtd.add(name2, group2)

        finalname = mtd.unique_name(prefix="final_")

        # add in the first workspace

        algo = Algo()
        algo.initialize()
        algo.diagnosticSuffix = {0: "_dspacing"}
        algo.setProperty("DiagnosticWorkspace", name1)
        algo.setProperty("TotalDiagnosticWorkspace", finalname)
        algo.setProperty("AddAtIndex", 0)
        algo.execute()

        finalGroup = mtd[finalname]
        wksp = finalGroup.getItem(0)
        assert_almost_equal(wksp, wksp1)

        # add in the second workspace

        algo = Algo()
        algo.initialize()
        algo.diagnosticSuffix = {0: "_dspacing"}
        algo.setProperty("DiagnosticWorkspace", name2)
        algo.setProperty("TotalDiagnosticWorkspace", finalname)
        algo.setProperty("AddAtIndex", 1)
        algo.execute()

        finalGroup = mtd[finalname]
        wksp = finalGroup.getItem(0)
        # for some reason the spectrum numbers do not correspond...
        assert_almost_equal(wksp, bigwksp, CheckSpectraMap=False)

    def test_simple_success_workspace_single_spectra(self):
        bigwksp = CreateWorkspace(
            DataX=[0, 1, 2, 0, 1, 2],
            DataY=[2, 2, 3, 3],
            NSpec=2,
        )
        # group 1
        name1 = mtd.unique_name(prefix="group1_")
        group1 = WorkspaceGroup()
        wksp1 = ExtractSingleSpectrum(
            InputWorkspace=bigwksp,
            OutputWorkspace=f"{name1}_dspacing",
            WorkspaceIndex=0,
        )
        group1.addWorkspace(wksp1)
        mtd.add(name1, group1)

        # group 2
        name2 = mtd.unique_name(prefix="group2_")
        group2 = WorkspaceGroup()
        wksp2 = ExtractSingleSpectrum(
            InputWorkspace=bigwksp,
            OutputWorkspace=f"{name2}_dspacing",
            WorkspaceIndex=1,
        )
        group2.addWorkspace(wksp2)
        mtd.add(name2, group2)

        finalname = mtd.unique_name(prefix="final_")

        # add in the first workspace

        algo = Algo()
        algo.initialize()
        algo.diagnosticSuffix = {0: "_dspacing"}
        algo.setProperty("DiagnosticWorkspace", name1)
        algo.setProperty("TotalDiagnosticWorkspace", finalname)
        algo.setProperty("AddAtIndex", 0)
        algo.execute()

        finalGroup = mtd[finalname]
        wksp = finalGroup.getItem(0)
        assert_almost_equal(wksp, wksp1)

        # add in the second workspace

        algo = Algo()
        algo.initialize()
        algo.diagnosticSuffix = {0: "_dspacing"}
        algo.setProperty("DiagnosticWorkspace", name2)
        algo.setProperty("TotalDiagnosticWorkspace", finalname)
        algo.setProperty("AddAtIndex", 1)
        algo.execute()

        finalGroup = mtd[finalname]
        wksp = finalGroup.getItem(0)
        # for some reason the spectrum numbers do not correspond...
        assert_almost_equal(wksp, bigwksp, CheckSpectraMap=False)

    def test_success(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        syntheticInputs = SyntheticData()
        ingredients = syntheticInputs.ingredients
        dBin = max([abs(d) for d in ingredients.pixelGroup.dBin()])

        runNumber = ingredients.runConfig.runNumber
        rawData = f"_test_groupcal_{runNumber}"
        groupingWorkspace = f"_test_groupcal_difc_{runNumber}"
        maskWorkspace = f"_test_groupcal_difc_{runNumber}_mask"
        difcWS = f"_{runNumber}_difcs_test"
        syntheticInputs.generateWorkspaces(rawData, groupingWorkspace, maskWorkspace)

        # create the DIFCprev table
        CalculateDiffCalTable(
            InputWorkspace=rawData,
            CalibrationTable=difcWS,
            OffsetMode="Signed",
            BinWidth=dBin,
        )

        DiffractionFocussing(
            InputWorkspace=rawData,
            GroupingWorkspace=groupingWorkspace,
            OutputWorkspace=rawData,
        )

        nDetectors = mtd[groupingWorkspace].getNumberHistograms()
        nGroups = mtd[rawData].getNumberHistograms()

        groupIDs = []
        groupedPeaks = {}
        groupedPeakBoundaries = {}
        for peakList in ingredients.groupedPeakLists:
            groupIDs.append(peakList.groupID)
            allPeaks = []
            allPeakBoundaries = []
            for peak in peakList.peaks:
                allPeaks.append(peak.value)
                allPeakBoundaries.append(peak.minimum)
                allPeakBoundaries.append(peak.maximum)
            groupedPeaks[peakList.groupID] = allPeaks
            groupedPeakBoundaries[peakList.groupID] = allPeakBoundaries

        finalname = mtd.unique_name(prefix="final_diag_")

        ref_col_names_position = set()
        ref_col_names_fitparam = set()
        ref_peak_num_fitparam = set()

        for index, groupID in enumerate(groupIDs):
            DIFCpd = mtd.unique_name(prefix=f"temp_DIFCgroup_{groupID}_")
            diagnosticWSgroup = mtd.unique_name(prefix=f"temp_diag_{groupID}_")
            PDCalibration(
                # in common with FitPeaks
                InputWorkspace=rawData,
                PeakFunction="Gaussian",
                PeakPositions=groupedPeaks[groupID],
                PeakWindow=groupedPeakBoundaries[groupID],
                BackgroundType="Linear",
                ConstrainPeakPositions=True,
                HighBackground=True,
                DiagnosticWorkspaces=diagnosticWSgroup,
                # specific to PDCalibration
                TofBinning=ingredients.pixelGroup.timeOfFlight.params,
                MaxChiSq=ingredients.maxChiSq,
                CalibrationParameters="DIFC",
                OutputCalibrationTable=DIFCpd,
                MaskWorkspace=maskWorkspace,
                # limit to specific spectrum
                StartWorkspaceIndex=index,
                StopWorkspaceIndex=index,
            )
            tempGroup = mtd[diagnosticWSgroup]
            tempGroup.sortByName()
            ref_col_names_position.update(tempGroup.getItem(0).getColumnNames())
            ref_col_names_fitparam.update(tempGroup.getItem(1).getColumnNames())
            ref_peak_num_fitparam.update(tempGroup.getItem(1).column("centre"))
            ConjoinDiagnosticWorkspaces(
                DiagnosticWorkspace=diagnosticWSgroup,
                TotalDiagnosticWorkspace=finalname,
                AddAtIndex=index,
            )

        finalGroup = mtd[finalname]
        # check the fitted peaks table
        tab1 = finalGroup.getItem(0)
        assert set(tab1.getColumnNames()) == ref_col_names_position
        assert set(tab1.column("detid")) == set(range(nDetectors))
        assert len(tab1.column("chisq")) == nDetectors
        for chi2 in tab1.column("chisq"):
            assert chi2 < 1.0e-5
        # check the fit paramerer table
        tab2 = finalGroup.getItem(1)
        assert set(tab2.getColumnNames()) == ref_col_names_fitparam
        assert len(tab2.column("peakindex")) == len(ref_peak_num_fitparam)
        nPeaks = int(len(ref_peak_num_fitparam) / nGroups)
        assert set(tab2.column("peakindex")) == set(range(nPeaks))
        assert set(tab2.column("wsindex")) == set(range(nGroups))

        # check the fitted peaks workspace
        wksp1 = finalGroup.getItem(2)
        assert wksp1.getNumberHistograms() == nGroups
