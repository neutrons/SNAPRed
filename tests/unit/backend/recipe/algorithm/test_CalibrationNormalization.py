import unittest
import unittest.mock as mock
from unittest.mock import call

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import (
        AddSampleLog,
        CalculateDIFC,
        CloneWorkspace,
        CreateEmptyTableWorkspace,
        CreateSampleWorkspace,
        DeleteWorkspace,
        LoadInstrument,
        Plus,
        Rebin,
    )
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
    from snapred.backend.dao.ingredients import (
        ReductionIngredients,
        SmoothDataExcludingPeaksIngredients,
    )
    from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
    from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
    from snapred.backend.dao.state.CalibrantSample.Material import Material
    from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import (
        CalibrationNormalization,  # noqa: E402
    )
    from snapred.meta.Config import Resource

    class TestCalibrationNormalizationAlgorithm(unittest.TestCase):
        def setUp(self):
            TOFBinParams = (1, 1, 100)
            self.sample_proton_charge = 10.0
            self.reductionIngredients = ReductionIngredients.parse_raw(
                Resource.read("/inputs/reduction/input_ingredients.json")
            )

            crystalInfo = CrystallographicInfo.parse_raw(Resource.read("inputs/purge_peaks/input_crystalInfo.json"))
            instrumentState = Calibration.parse_raw(
                Resource.read("inputs/purge_peaks/input_parameters.json")
            ).instrumentState
            self.smoothIngredients = SmoothDataExcludingPeaksIngredients(
                crystalInfo=crystalInfo, instrumentState=instrumentState
            )
            material = Material(
                chemicalFormula="V",
            )
            cylinder = Geometry(
                shape="Cylinder",
                radius=0.15,
                height=0.3,
            )

            self.calibrantSample = CalibrantSamples(
                name="vanadium cylinder",
                unique_id="435elmst",
                geometry=cylinder,
                material=material,
            )

            # create some sample data
            self.backgroundWS = "_test_data_raw_vanadium_background"
            self.sampleWS = "_test_data_raw_vanadium"
            CreateSampleWorkspace(
                OutputWorkspace=self.backgroundWS,
                # WorkspaceType="Histogram",
                Function="User Defined",
                UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
                Xmin=TOFBinParams[0],
                Xmax=TOFBinParams[2],
                BinWidth=TOFBinParams[1],
                XUnit="TOF",
                NumBanks=4,  # must produce same number of pixels as fake instrument
                BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
                Random=True,
            )
            # add proton charge for current normalization
            AddSampleLog(
                Workspace=self.backgroundWS,
                LogName="gd_prtn_chrg",
                LogText=f"{self.sample_proton_charge}",
                LogType="Number",
            )
            # load an instrument into sample data
            LoadInstrument(
                Workspace=self.backgroundWS,
                Filename=Resource.getPath("inputs/diffcal/fakeSNAPLite.xml"),
                InstrumentName="fakeSNAPLite",
                RewriteSpectraMap=False,
            )

            CloneWorkspace(
                InputWorkspace=self.backgroundWS,
                OutputWorkspace=self.sampleWS,
            )
            CreateSampleWorkspace(
                OutputWorkspace="_tmp_raw_vanadium",
                # WorkspaceType="Histogram",
                Function="User Defined",
                UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=70,Sigma=1",
                Xmin=TOFBinParams[0],
                Xmax=TOFBinParams[2],
                BinWidth=TOFBinParams[1],
                XUnit="TOF",
                NumBanks=4,  # must produce same number of pixels as fake instrument
                BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
                Random=True,
            )
            Plus(
                LHSWorkspace="_tmp_raw_vanadium",
                RHSWorkspace=self.sampleWS,
                OutputWorkspace=self.sampleWS,
            )
            DeleteWorkspace("_tmp_raw_vanadium")

            self.difcWS = "_difc_table_raw_vanadium"
            ws = CalculateDIFC(
                InputWorkspace=self.sampleWS,
                OutputWorkspace=self.difcWS,
                OffsetMode="Signed",
                BinWidth=TOFBinParams[1],
            )
            difc = CreateEmptyTableWorkspace()
            difc.addColumn("int", "detid")
            difc.addColumn("double", "difc")
            difc.addColumn("double", "tzero")
            difc.addColumn("double", "difa")
            for i in range(ws.getNumberHistograms()):
                difc.addRow([i + 1, ws.readY(i)[0], 0.0, 0.0])
            DeleteWorkspace(self.difcWS)
            self.difcWS = difc.name()

            Rebin(
                InputWorkspace=self.sampleWS,
                Params=TOFBinParams,
                PreserveEvents=False,
                OutputWorkspace=self.sampleWS,
                BinningMode="Logarithmic",
            )
            Rebin(
                InputWorkspace=self.backgroundWS,
                Params=TOFBinParams,
                PreserveEvents=False,
                OutputWorkspace=self.backgroundWS,
                BinningMode="Logarithmic",
            )

        def test_init(self):
            """Test ability to initialize vanadium focussed reduction algo"""
            outputWSName = "_test_workspace_raw_vanadium"
            normalAlgo = CalibrationNormalization()
            normalAlgo.initialize()
            normalAlgo.setProperty("ReductionIngredients", self.reductionIngredients.json())
            normalAlgo.setProperty("SmoothDataIngredients", self.smoothIngredients.json())
            normalAlgo.setProperty("CalibrantSample", self.calibrantSample.json())
            normalAlgo.setProperty("InputWorkspace", self.sampleWS)
            normalAlgo.setProperty("BackgroundWorkspace", self.backgroundWS)
            normalAlgo.setProperty("CalibrationWorkspace", self.difcWS)
            normalAlgo.setProperty("OutputWorkspace", outputWSName)
            assert normalAlgo.getProperty("ReductionIngredients").value == self.reductionIngredients.json()
            assert normalAlgo.getProperty("SmoothDataIngredients").value == self.smoothIngredients.json()
            assert normalAlgo.getPropertyValue("InputWorkspace") == self.sampleWS
            assert normalAlgo.getPropertyValue("BackgroundWorkspace") == self.backgroundWS
            assert normalAlgo.getProperty("CalibrantSample").value == self.calibrantSample.json()
            assert normalAlgo.getPropertyValue("OutputWorkspace") == outputWSName

        @mock.patch("snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo.mtd")
        @mock.patch("snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo.MantidSnapper")
        def test_execute(self, mock_MantidSnapper, mock_mtd):
            normalAlgo = CalibrationNormalization()
            mock_mtd.side_effect = {"diffraction_focused_vanadium": ["ws1", "ws2"]}
            normalAlgo.initialize()
            normalAlgo.setProperty("ReductionIngredients", self.reductionIngredients.json())
            normalAlgo.setProperty("SmoothDataIngredients", self.smoothIngredients.json())
            normalAlgo.setProperty("CalibrantSample", self.calibrantSample.json())
            normalAlgo.setProperty("InputWorkspace", self.sampleWS)
            normalAlgo.setProperty("BackgroundWorkspace", self.backgroundWS)
            normalAlgo.setProperty("CalibrationWorkspace", self.difcWS)
            normalAlgo.setProperty("OutputWorkspace", "_test_workspace_raw_vanadium")
            normalAlgo.execute()

            wsGroupName = normalAlgo.getProperty("FocusWorkspace").value
            assert wsGroupName == "ws"
            expected_calls = [
                call().RawVanadiumCorrectionAlgorithm,
                call().CustomGroupWorkspace,
                call().RebinRagged,
                call().RebinRagged,
                call().RebinRagged,
                call().DiffractionFocussing,
                call().CloneWorkspace,
                call().SmoothDataExcludingPeaks,
                call().executeQueue,
            ]

            actual_calls = [call[0] for call in mock_MantidSnapper.mock_calls if call[0]]
            print(actual_calls)

            # Assertions
            assert actual_calls == [call[0] for call in expected_calls]
