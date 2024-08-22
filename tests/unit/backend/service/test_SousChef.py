import os.path
import tempfile
import unittest
from unittest import mock

from mantid.simpleapi import DeleteWorkspace, mtd
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef
from snapred.meta.Config import Config

thisService = "snapred.backend.service.SousChef."


class TestSousChef(unittest.TestCase):
    def setUp(self):
        self.instance = SousChef()
        self.ingredients = FarmFreshIngredients(
            runNumber="123",
            useLiteMode=True,
            focusGroups=[{"name": "apple", "definition": "banana/coconut"}],
            calibrantSamplePath="path/to/sample.xyz",
            cifPath="path/to/cif",
            maxChiSq=100.0,
        )

    def tearDown(self):
        del self.instance
        del self.ingredients

    @classmethod
    def tearDownClass(cls):
        for ws in mtd.getObjectNames():
            DeleteWorkspace(ws)

    def test_prepManyDetectorPeaks(self):
        self.instance.prepDetectorPeaks = mock.Mock()
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)
        res = self.instance.prepManyDetectorPeaks(self.ingredients)
        assert res[0] == self.instance.prepDetectorPeaks.return_value
        self.instance.prepDetectorPeaks.assert_called_once_with(self.ingredients, purgePeaks=False)

    def test_prepManyDetectorPeaks_no_calibration(self):
        self.instance.prepDetectorPeaks = mock.Mock()
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=False)
        self.instance.logger = mock.Mock()
        self.instance.logger.warning = mock.Mock()

        res = self.instance.prepManyDetectorPeaks(self.ingredients)
        assert res is None
        self.instance.logger().warning.assert_called_once_with("No calibration record found for run 123 in lite mode.")

    def test_prepManyPixelGroups(self):
        self.instance.prepPixelGroup = mock.Mock()
        res = self.instance.prepManyPixelGroups(self.ingredients)
        assert res[0] == self.instance.prepPixelGroup.return_value
        self.instance.prepPixelGroup.assert_called_once_with(self.ingredients)

    def test_prepFocusGroup_exists(self):
        # create a temp file to be used a the path for the focus group
        # since it exists, prepFocusGroup should jsut return the focusGroup in the ingredients
        with tempfile.NamedTemporaryFile(suffix=".xml") as existingFocusGroup:
            self.ingredients.focusGroup.definition = existingFocusGroup.name
            res = self.instance.prepFocusGroup(self.ingredients)
            assert res == self.ingredients.focusGroup

    def test_prepFocusGroup_notExists(self):
        # ensure the file does not exist
        # make sure the grouping map is accessed in this case instead

        # create the mock grouping map dictionary
        mockGroupingDictionary = {self.ingredients.focusGroup.name: "passed"}
        mockGroupingMap = mock.Mock(getMap=mock.Mock(return_value=mockGroupingDictionary))
        self.instance.dataFactoryService.getGroupingMap = mock.Mock(return_value=mockGroupingMap)

        # ensure the file does not exist by looking inside a temporary file
        with tempfile.TemporaryDirectory() as tmpdir:
            self.ingredients.focusGroup.definition = tmpdir + "/muffin.egg"
            assert not os.path.isfile(self.ingredients.focusGroup.definition)
            res = self.instance.prepFocusGroup(self.ingredients)

        assert res == mockGroupingDictionary[self.ingredients.focusGroup.name]

    def test_prepCalibration(self):
        mockCalibration = mock.Mock()
        self.instance.dataFactoryService.getCalibrationState = mock.Mock(return_value=mockCalibration)
        self.instance.prepCalibrantSample = mock.Mock()

        res = self.instance.prepCalibration(self.ingredients)

        self.instance.dataFactoryService.getCalibrationState.assert_called_once_with(
            self.ingredients.runNumber,
            self.ingredients.useLiteMode,
        )
        assert res == self.instance.dataFactoryService.getCalibrationState.return_value
        assert res.instrumentState.fwhmMultipliers.dict() == Config["calibration.parameters.default.FWHMMultiplier"]

    def test_prepCalibration_userFWHM(self):
        mockCalibration = mock.Mock()
        self.instance.dataFactoryService.getCalibrationState = mock.Mock(return_value=mockCalibration)
        fakeLeft = 116
        fakeRight = 17
        self.ingredients.fwhmMultipliers = mock.Mock(left=fakeLeft, right=fakeRight)
        self.instance.prepCalibrantSample = mock.Mock()

        res = self.instance.prepCalibration(self.ingredients)

        self.instance.dataFactoryService.getCalibrationState.assert_called_once_with(
            self.ingredients.runNumber,
            self.ingredients.useLiteMode,
        )
        assert res == self.instance.dataFactoryService.getCalibrationState.return_value
        assert res.instrumentState.fwhmMultipliers == self.ingredients.fwhmMultipliers
        assert res.instrumentState.fwhmMultipliers.left == fakeLeft
        assert res.instrumentState.fwhmMultipliers.right == fakeRight

    def test_prepInstrumentState(self):
        ingredients = mock.Mock()

        mockCalibration = mock.Mock(instrumentState=mock.Mock())
        self.instance.prepCalibration = mock.Mock(return_value=mockCalibration)
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)
        res = self.instance.prepInstrumentState(ingredients)
        self.instance.prepCalibration.assert_called_once_with(ingredients)
        assert res == self.instance.prepCalibration.return_value.instrumentState

    def test_prepDefaultInstrumentState(self):
        ingredients = mock.Mock(
            spec=FarmFreshIngredients,
            runNumber="12345",
            useLiteMode=True,
        )
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=False)
        self.instance.dataFactoryService.getDefaultInstrumentState = mock.Mock(return_value=mock.Mock())
        result = self.instance.prepInstrumentState(ingredients)
        self.instance.dataFactoryService.calibrationExists.assert_called_once_with(
            ingredients.runNumber,
            ingredients.useLiteMode,
        )
        self.instance.dataFactoryService.getDefaultInstrumentState.assert_called_once_with(
            ingredients.runNumber,
        )
        assert result == self.instance.dataFactoryService.getDefaultInstrumentState.return_value

    def test_prepRunConfig(self):
        self.instance.dataFactoryService.lookupService.readRunConfig = mock.Mock()
        res = self.instance.prepRunConfig(self.ingredients.runNumber)
        assert res == self.instance.dataFactoryService.lookupService.readRunConfig.return_value

    def test_prepCalibrantSample(self):
        self.instance.dataFactoryService.lookupService.readCalibrantSample = mock.Mock()
        res = self.instance.prepCalibrantSample(self.ingredients)
        assert res == self.instance.dataFactoryService.lookupService.readCalibrantSample.return_value

    @mock.patch(thisService + "PixelGroup")
    @mock.patch(thisService + "PixelGroupingParametersCalculationRecipe")
    @mock.patch(thisService + "PixelGroupingIngredients")
    def test_prepPixelGroup_nocache(
        self,
        PixelGroupingIngredients,
        PixelGroupingParametersCalculationRecipe,
        PixelGroup,
    ):
        self.instance = SousChef()
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)
        key = (self.ingredients.runNumber, self.ingredients.useLiteMode, self.ingredients.focusGroup.name)
        # ensure there is no cached value
        assert self.instance._pixelGroupCache == {}

        # mock the calibration, which will give the instrument state
        mockCalibration = mock.Mock(instrumentState=mock.Mock())
        self.instance.prepCalibration = mock.Mock(return_value=mockCalibration)
        self.instance.groceryService.fetchGroceryDict = mock.Mock(
            return_value={"groupingWorkspace", self.ingredients.focusGroup.name},
        )

        # call the method to be tested
        # make sure the focus group definition exists, by pointing it to a tmp file
        with tempfile.NamedTemporaryFile() as existent:
            self.ingredients.focusGroup.definition = existent.name
            result = self.instance.prepPixelGroup(self.ingredients)

        # make necessary assertions
        PixelGroupingIngredients.assert_called_once_with(
            instrumentState=self.instance.prepCalibration.return_value.instrumentState,
            nBinsAcrossPeakWidth=self.ingredients.nBinsAcrossPeakWidth,
        )
        PixelGroupingParametersCalculationRecipe.return_value.executeRecipe.assert_called_once_with(
            PixelGroupingIngredients.return_value,
            self.instance.groceryService.fetchGroceryDict.return_value,
        )
        assert self.instance._pixelGroupCache == {key: PixelGroup.return_value}
        assert result == self.instance._pixelGroupCache[key]

    @mock.patch(thisService + "PixelGroupingParametersCalculationRecipe")
    def test_prepPixelGroup_cache(self, PixelGroupingParametersCalculationRecipe):
        key = (self.ingredients.runNumber, self.ingredients.useLiteMode, self.ingredients.focusGroup.name)
        # ensure the cache is prepared
        self.instance._pixelGroupCache[key] = mock.Mock()

        res = self.instance.prepPixelGroup(self.ingredients)

        assert not PixelGroupingParametersCalculationRecipe.called
        assert res == self.instance._pixelGroupCache[key]

    def test_getInstrumentDefinitionFilename(self):
        assert Config["instrument.lite.definition.file"] == self.instance._getInstrumentDefinitionFilename(True)
        assert Config["instrument.native.definition.file"] == self.instance._getInstrumentDefinitionFilename(False)

    @mock.patch(thisService + "CrystallographicInfoService")
    def test_prepXtalInfo_nocache(self, XtalService):
        key = (
            self.ingredients.cifPath,
            self.ingredients.crystalDBounds.minimum,
            self.ingredients.crystalDBounds.maximum,
        )
        # ensure the cache is clear
        assert self.instance._xtalCache == {}

        res = self.instance.prepCrystallographicInfo(self.ingredients)

        XtalService.return_value.ingest.assert_called_once_with(*key)
        assert self.instance._xtalCache == {key: XtalService.return_value.ingest.return_value["crystalInfo"]}
        assert res == self.instance._xtalCache[key]

    @mock.patch(thisService + "CrystallographicInfoService")
    def test_prepXtalInfo_cache(self, XtalService):
        key = (
            self.ingredients.cifPath,
            self.ingredients.crystalDBounds.minimum,
            self.ingredients.crystalDBounds.maximum,
        )
        # ensure the cache is preped
        self.instance._xtalCache[key] = mock.Mock()

        res = self.instance.prepCrystallographicInfo(self.ingredients)

        assert not XtalService.called
        assert res == self.instance._xtalCache[key]

    @mock.patch(thisService + "CrystallographicInfoService")
    def test_prepXtalInfo_noCif(self, XtalService):
        key = (
            self.ingredients.cifPath,
            self.ingredients.crystalDBounds.minimum,
            self.ingredients.crystalDBounds.maximum,
        )
        # ensure the cache is preped
        self.instance._xtalCache[key] = mock.Mock()

        # make ingredients with no CIF path
        incompleteIngredients = self.ingredients.model_copy()
        incompleteIngredients.cifPath = None

        # mock out the data factory
        self.instance.dataFactoryService = mock.Mock()
        self.instance.dataFactoryService.getCifFilePath.return_value = self.ingredients.cifPath

        res = self.instance.prepCrystallographicInfo(incompleteIngredients)

        assert not XtalService.called
        assert res == self.instance._xtalCache[key]
        assert self.instance.dataFactoryService.getCifFilePath.called

    @mock.patch(thisService + "PeakIngredients")
    def test_prepPeakIngredients(self, PeakIngredients):
        self.instance.prepCrystallographicInfo = mock.Mock()
        self.instance.prepInstrumentState = mock.Mock()
        self.instance.prepPixelGroup = mock.Mock()
        self.instance.prepCalibrantSample = mock.Mock()
        calibrantSample = self.instance.prepCalibrantSample()
        self.ingredients.peakIntensityThreshold = calibrantSample.peakIntensityFractionThreshold

        result = self.instance.prepPeakIngredients(self.ingredients)

        self.instance.prepCrystallographicInfo.assert_called_once_with(self.ingredients)
        self.instance.prepInstrumentState.assert_called_once_with(self.ingredients)
        self.instance.prepPixelGroup.assert_called_once_with(self.ingredients)
        PeakIngredients.assert_called_once_with(
            crystalInfo=self.instance.prepCrystallographicInfo.return_value,
            instrumentState=self.instance.prepInstrumentState.return_value,
            pixelGroup=self.instance.prepPixelGroup.return_value,
            peakIntensityThreshold=calibrantSample.peakIntensityFractionThreshold,
        )
        assert result == PeakIngredients.return_value

    @mock.patch(thisService + "DetectorPeakPredictorRecipe")
    @mock.patch(thisService + "GroupPeakList")
    def test_prepDetectorPeaks_nocache_nopurge(self, GroupPeakList, DetectorPeakPredictorRecipe):  # noqa: ARG002
        key = (
            self.ingredients.runNumber,
            self.ingredients.useLiteMode,
            self.ingredients.focusGroup.name,
            self.ingredients.crystalDBounds.minimum,
            self.ingredients.crystalDBounds.maximum,
            self.ingredients.fwhmMultipliers.left,
            self.ingredients.fwhmMultipliers.right,
            False,
        )
        # ensure the cache is clear
        assert self.instance._peaksCache == {}

        self.instance.prepCalibrantSample = mock.Mock()
        self.instance.prepPeakIngredients = mock.Mock()
        self.instance.parseGroupPeakList = mock.Mock(side_effect=lambda y: [y])

        res = self.instance.prepDetectorPeaks(self.ingredients, False)

        self.instance.prepPeakIngredients.assert_called_once_with(self.ingredients)
        DetectorPeakPredictorRecipe.return_value.executeRecipe.assert_called_once_with(
            Ingredients=self.instance.prepPeakIngredients.return_value,
        )
        assert res == [DetectorPeakPredictorRecipe.return_value.executeRecipe.return_value]
        assert self.instance._peaksCache == {key: [DetectorPeakPredictorRecipe.return_value.executeRecipe.return_value]}

    @mock.patch(thisService + "PurgeOverlappingPeaksRecipe")
    @mock.patch(thisService + "DetectorPeakPredictorRecipe")
    @mock.patch(thisService + "GroupPeakList")
    def test_prepDetectorPeaks_nocache_purge(
        self,
        GroupPeakList,  # noqa: ARG002
        DetectorPeakPredictorRecipe,
        PurgeOverlappingPeaksRecipe,
    ):  # noqa: ARG002
        key = (
            self.ingredients.runNumber,
            self.ingredients.useLiteMode,
            self.ingredients.focusGroup.name,
            self.ingredients.crystalDBounds.minimum,
            self.ingredients.crystalDBounds.maximum,
            self.ingredients.fwhmMultipliers.left,
            self.ingredients.fwhmMultipliers.right,
            True,
        )
        # ensure the cache is clear
        assert self.instance._peaksCache == {}

        self.instance.prepCalibrantSample = mock.Mock()
        self.instance.prepPeakIngredients = mock.Mock()
        self.instance.parseGroupPeakList = mock.Mock(side_effect=lambda y: [y])

        res = self.instance.prepDetectorPeaks(self.ingredients)

        self.instance.prepPeakIngredients.assert_called_once_with(self.ingredients)
        DetectorPeakPredictorRecipe.return_value.executeRecipe.assert_called_once_with(
            Ingredients=self.instance.prepPeakIngredients.return_value,
        )
        assert res == [PurgeOverlappingPeaksRecipe.return_value.executeRecipe.return_value]
        assert self.instance._peaksCache == {key: [PurgeOverlappingPeaksRecipe.return_value.executeRecipe.return_value]}

    @mock.patch(thisService + "DetectorPeakPredictorRecipe")
    def test_prepDetectorPeaks_cache(self, DetectorPeakPredictorRecipe):
        key = (
            self.ingredients.runNumber,
            self.ingredients.useLiteMode,
            self.ingredients.focusGroup.name,
            self.ingredients.crystalDBounds.minimum,
            self.ingredients.crystalDBounds.maximum,
            self.ingredients.fwhmMultipliers.left,
            self.ingredients.fwhmMultipliers.right,
            True,
        )
        # ensure the cache is prepared
        self.instance._peaksCache[key] = [mock.Mock()]
        self.instance.prepCalibrantSample = mock.Mock()

        res = self.instance.prepDetectorPeaks(self.ingredients)

        assert not DetectorPeakPredictorRecipe.called
        assert res == self.instance._peaksCache[key]

    @mock.patch(thisService + "ReductionIngredients")
    def test_prepReductionIngredients(self, ReductionIngredients):
        calibrationCalibrantSamplePath = "a/sample.x"
        record = mock.Mock(
            smoothingParamter=1.0,
            calculationParameters=mock.Mock(
                calibrantSamplePath=calibrationCalibrantSamplePath,
            ),
        )
        self.instance.prepCalibrantSample = mock.Mock()
        self.instance.prepRunConfig = mock.Mock()
        self.instance.prepManyPixelGroups = mock.Mock()
        self.instance.prepManyDetectorPeaks = mock.Mock()
        self.instance.dataFactoryService.getCifFilePath = mock.Mock()
        self.instance.dataFactoryService.getReductionState = mock.Mock()
        self.instance.dataFactoryService.getNormalizationRecord = mock.Mock(return_value=record)
        self.instance.dataFactoryService.getCalibrationRecord = mock.Mock(return_value=record)

        ingredients_ = self.ingredients.model_copy()
        # ... from calibration record:
        ingredients_.calibrantSamplePath = calibrationCalibrantSamplePath
        ingredients_.cifPath = self.instance.dataFactoryService.getCifFilePath.return_value
        # ... from normalization record:
        ingredients_.peakIntensityThreshold = self.instance._getThresholdFromCalibrantSample(
            "calibrationCalibrantSamplePath"
        )
        result = self.instance.prepReductionIngredients(ingredients_)

        self.instance.prepManyPixelGroups.assert_called_once_with(ingredients_)
        self.instance.dataFactoryService.getCifFilePath.assert_called_once_with("sample")
        ReductionIngredients.assert_called_once_with(
            pixelGroups=self.instance.prepManyPixelGroups.return_value,
            smoothingParameter=record.smoothingParameter,
            calibrantSamplePath=ingredients_.calibrantSamplePath,
            peakIntensityThreshold=ingredients_.peakIntensityThreshold,
            detectorPeaksMany=self.instance.prepManyDetectorPeaks.return_value,
            keepUnfocused=ingredients_.keepUnfocused,
            convertUnitsTo=ingredients_.convertUnitsTo,
        )
        assert result == ReductionIngredients.return_value

    @mock.patch(thisService + "NormalizationIngredients")
    def test_prepNormalizationIngredients(self, NormalizationIngredients):
        self.instance.prepCalibrantSample = mock.Mock()
        self.instance.prepPixelGroup = mock.Mock()
        self.instance.prepDetectorPeaks = mock.Mock()
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)

        res = self.instance.prepNormalizationIngredients(self.ingredients)

        self.instance.prepCalibrantSample.assert_called_once_with(self.ingredients.calibrantSamplePath)
        self.instance.prepPixelGroup.assert_called_once_with(self.ingredients)
        self.instance.prepDetectorPeaks.assert_called_once_with(self.ingredients, purgePeaks=False)
        NormalizationIngredients.assert_called_once_with(
            pixelGroup=self.instance.prepPixelGroup.return_value,
            calibrantSample=self.instance.prepCalibrantSample.return_value,
            detectorPeaks=self.instance.prepDetectorPeaks.return_value,
        )
        assert res == NormalizationIngredients.return_value

    @mock.patch(thisService + "DiffractionCalibrationIngredients")
    def test_prepDiffractionCalibrationIngredients(self, DiffractionCalibrationIngredients):
        self.instance.prepRunConfig = mock.Mock()
        self.instance.prepPixelGroup = mock.Mock()
        self.instance.prepDetectorPeaks = mock.Mock()
        self.instance.dataFactoryService.calibrationExists = mock.Mock(return_value=True)

        result = self.instance.prepDiffractionCalibrationIngredients(self.ingredients)

        self.instance.prepRunConfig.assert_called_once_with(self.ingredients.runNumber)
        self.instance.prepPixelGroup.assert_called_once_with(self.ingredients)
        self.instance.prepDetectorPeaks.assert_called_once_with(self.ingredients)
        DiffractionCalibrationIngredients.assert_called_once_with(
            runConfig=self.instance.prepRunConfig.return_value,
            pixelGroup=self.instance.prepPixelGroup.return_value,
            groupedPeakLists=self.instance.prepDetectorPeaks.return_value,
            peakFunction=self.ingredients.peakFunction,
            convergenceThreshold=self.ingredients.convergenceThreshold,
            maxOffset=self.ingredients.maxOffset,
            maxChiSq=self.ingredients.maxChiSq,
        )
        assert result == DiffractionCalibrationIngredients.return_value

    def test_pullManyCalibrationDetectorPeaks(self):
        mockDataFactory = mock.Mock()
        mockDataFactory.getCalibrationRecord = mock.Mock()
        mockDataFactory.getCalibrationRecord.return_value = mock.Mock()
        self.instance.dataFactoryService = mockDataFactory
        self.instance.prepManyDetectorPeaks = mock.Mock()
        self.instance._pullCalibrationRecordFFI = mock.Mock()

        self.ingredients.cifPath = None

        res = self.instance._pullManyCalibrationDetectorPeaks(self.ingredients, "12345", True)
        assert res == self.instance.prepManyDetectorPeaks.return_value
        self.instance.prepManyDetectorPeaks.assert_called_once_with(
            self.instance._pullCalibrationRecordFFI.return_value
        )
        self.instance._pullCalibrationRecordFFI.assert_called_once_with(self.ingredients, "12345", True)

    @mock.patch("os.path.exists", return_value=True)
    def test__getThresholdFromCalibrantSample(self, mockOS):  # noqa: ARG002
        self.instance.prepCalibrantSample = mock.Mock()
        calibrantSample = self.instance.prepCalibrantSample()
        calibrantSample.peakIntensityFractionThreshold = mock.Mock()
        path = self.ingredients.calibrantSamplePath
        result = self.instance._getThresholdFromCalibrantSample(path)
        assert result == calibrantSample.peakIntensityFractionThreshold

    def test__getThresholdFromCalibrantSample_none_path(self):
        result = self.instance._getThresholdFromCalibrantSample(None)
        assert result == Config["constants.PeakIntensityFractionThreshold"]
