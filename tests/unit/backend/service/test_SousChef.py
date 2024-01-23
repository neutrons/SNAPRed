import unittest
from unittest import mock

import pytest
from mantid.simpleapi import DeleteWorkspace, mtd
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.service.SousChef import SousChef
from snapred.meta.Config import Config

thisService = "snapred.backend.service.SousChef."


class TestSousChef(unittest.TestCase):
    def setUp(self):
        self.instance = SousChef()
        self.ingredients = FarmFreshIngredients(
            runNumber="123",
            useLiteMode=True,
            focusGroup={"name": "apple", "definition": "banana/coconut"},
            calibrantSamplePath="path/to/sample",
            cifPath="path/to/cif",
        )

    def tearDown(self):
        del self.instance
        del self.ingredients

    @classmethod
    def tearDownClass(cls):
        for ws in mtd.getObjectNames():
            DeleteWorkspace(ws)

    def test_prepCalibration_nocache(self):
        runNumber = self.ingredients.runNumber
        assert self.instance._calibrationCache == {}

        mockCalibration = mock.Mock()
        self.instance.dataFactoryService.getCalibrationState = mock.Mock(return_value=mockCalibration)

        res = self.instance.prepCalibration(runNumber)

        assert self.instance.dataFactoryService.getCalibrationState.called_once_with(runNumber)
        assert res == self.instance.dataFactoryService.getCalibrationState.return_value

    def test_prepCalibration_cached(self):
        key = self.ingredients.runNumber
        # prepare the cache
        self.instance._calibrationCache[key] = mock.Mock()

        self.instance.dataFactoryService.getCalibrantSample = mock.Mock()
        res = self.instance.prepCalibration(self.ingredients.runNumber)

        assert not self.instance.dataFactoryService.getCalibrantSample.called
        assert res == self.instance._calibrationCache[key]

    def test_prepInstrumentState(self):
        runNumber = "123"
        mockCalibration = mock.Mock(instrumentState=mock.Mock())
        self.instance.prepCalibration = mock.Mock(return_value=mockCalibration)
        res = self.instance.prepInstrumentState(runNumber)
        assert self.instance.prepCalibration.called_once_with(runNumber)
        assert res == self.instance.prepCalibration.return_value.instrumentState

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
        key = (self.ingredients.runNumber, self.ingredients.useLiteMode, self.ingredients.focusGroup.name)
        # ensure there is no cached value
        assert self.instance._pixelGroupCache == {}

        # mock the calibration, which will give the instrument state
        mockCalibration = mock.Mock(instrumentState=mock.Mock())
        self.instance.prepCalibration = mock.Mock(return_value=mockCalibration)
        self.instance.groceryService.fetchGroceryList = mock.Mock(return_value="banana")

        # call the method to be tested
        res = self.instance.prepPixelGroup(self.ingredients)

        # make necessary assertions
        assert PixelGroupingIngredients.called_once_with(
            instrumentState=self.instance.prepCalibration.return_value.instrumentState,
            nBinsAcrossPeakWidth=self.ingredients.nBinsAcrossPeakWidth,
        )
        assert PixelGroupingParametersCalculationRecipe.return_value.executeRecipe.called_once_with(
            PixelGroupingIngredients.return_value,
            self.instance.groceryService.fetchGroceryList.return_value,
        )
        assert self.instance._pixelGroupCache == {key: PixelGroup.return_value}
        assert res == self.instance._pixelGroupCache[key]

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
        key = (self.ingredients.cifPath, self.ingredients.dBounds.minimum, self.ingredients.dBounds.maximum)
        # ensure the cache is clear
        assert self.instance._xtalCache == {}

        res = self.instance.prepCrystallographicInfo(self.ingredients)

        assert XtalService.return_value.ingest.called_once_with(key)
        assert self.instance._xtalCache == {key: XtalService.return_value.ingest.return_value["crystalInfo"]}
        assert res == self.instance._xtalCache[key]

    @mock.patch(thisService + "CrystallographicInfoService")
    def test_prepXtalInfo_cache(self, XtalService):
        key = (self.ingredients.cifPath, self.ingredients.dBounds.minimum, self.ingredients.dBounds.maximum)
        # ensure the cache is preped
        self.instance._xtalCache[key] = mock.Mock()

        res = self.instance.prepCrystallographicInfo(self.ingredients)

        assert not XtalService.called
        assert res == self.instance._xtalCache[key]

    @mock.patch(thisService + "PeakIngredients")
    def test_prepPeakIngredients(self, PeakIngredients):
        self.instance.prepCrystallographicInfo = mock.Mock()
        self.instance.prepInstrumentState = mock.Mock()
        self.instance.prepPixelGroup = mock.Mock()

        res = self.instance.prepPeakIngredients(self.ingredients)

        assert self.instance.prepCrystallographicInfo.called_once_with(self.ingredients)
        assert self.instance.prepInstrumentState.called_once_with(self.ingredients.runNumber)
        assert self.instance.prepPixelGroup.called_once_with(self.ingredients)
        assert PeakIngredients.called_once_with(
            crystalInfo=self.instance.prepCrystallographicInfo.return_value,
            instrumentState=self.instance.prepInstrumentState.return_value,
            pixelGroup=self.instance.prepPixelGroup.return_value,
            peakIntensityThreshold=self.ingredients.peakIntensityThreshold,
        )
        assert res == PeakIngredients.return_value

    @mock.patch(thisService + "DetectorPeakPredictorRecipe")
    @mock.patch(thisService + "parse_raw_as")
    @mock.patch(thisService + "GroupPeakList")
    def test_prepDetectorPeaks_nocache(self, GroupPeakList, parse_raw_as, DetectorPeakPredictorRecipe):  # noqa: ARG002
        key = (
            self.ingredients.runNumber,
            self.ingredients.useLiteMode,
            self.ingredients.focusGroup.name,
            self.ingredients.peakIntensityThreshold,
        )
        # ensure the cache is clear
        assert self.instance._peaksCache == {}

        self.instance.prepPeakIngredients = mock.Mock()
        parse_raw_as.side_effect = lambda x, y: [y]  # noqa: ARG005

        res = self.instance.prepDetectorPeaks(self.ingredients)

        assert self.instance.prepPeakIngredients.called_once_with(self.ingredients)
        assert DetectorPeakPredictorRecipe.return_value.executeRecipe.called_once_with(Ingredients=self.ingredients)
        assert res == [DetectorPeakPredictorRecipe.return_value.executeRecipe.return_value]
        assert self.instance._peaksCache == {key: [DetectorPeakPredictorRecipe.return_value.executeRecipe.return_value]}

    @mock.patch(thisService + "DetectorPeakPredictorRecipe")
    def test_prepDetectorPeaks_cache(self, DetectorPeakPredictorRecipe):
        key = (
            self.ingredients.runNumber,
            self.ingredients.useLiteMode,
            self.ingredients.focusGroup.name,
            self.ingredients.peakIntensityThreshold,
        )
        # ensure the cache is prepared
        self.instance._peaksCache[key] = [mock.Mock()]

        res = self.instance.prepDetectorPeaks(self.ingredients)

        assert not DetectorPeakPredictorRecipe.called
        assert res == self.instance._peaksCache[key]

    @mock.patch(thisService + "ReductionIngredients")
    def test_prepReductionIngredients(self, ReductionIngredients):
        self.instance.prepRunConfig = mock.Mock()
        self.instance.prepPixelGroup = mock.Mock()
        self.instance.dataFactoryService.getReductionState = mock.Mock()

        res = self.instance.prepReductionIngredients(self.ingredients)

        assert self.instance.prepRunConfig.called_once_with(self.ingredients.runNumber)
        assert self.instance.prepPixelGroup.called_once_with(self.ingredients)
        assert self.instance.dataFactoryService.getReductionState.called_once_with(self.ingredients.runNumber)
        assert ReductionIngredients.called_once_with(
            reductionState=self.instance.dataFactoryService.getReductionState.return_value,
            runConfig=self.instance.prepRunConfig.return_value,
            pixelGroup=self.instance.prepPixelGroup.return_value,
        )
        assert res == ReductionIngredients.return_value

    @mock.patch(thisService + "NormalizationIngredients")
    def test_prepNormalizationIngredients(self, NormalizationIngredients):
        self.instance.prepCalibrantSample = mock.Mock()
        self.instance.prepPixelGroup = mock.Mock()
        self.instance.prepDetectorPeaks = mock.Mock()

        res = self.instance.prepNormalizationIngredients(self.ingredients)

        assert self.instance.prepCalibrantSample.called_once_with(self.ingredients.calibrantSamplePath)
        assert self.instance.prepPixelGroup.called_once_with(self.ingredients)
        assert self.instance.prepDetectorPeaks.called_once_with(self.ingredients)
        assert NormalizationIngredients.called_once_with(
            pixelGroup=self.instance.prepPixelGroup.return_value,
            calibrantSamplePath=self.instance.prepCalibrantSample.return_value,
            detectorPeakskLists=self.instance.prepDetectorPeaks.return_value,
        )
        assert res == NormalizationIngredients.return_value

    @mock.patch(thisService + "DiffractionCalibrationIngredients")
    def test_prepDiffractionCalibrationIngredients(self, DiffractionCalibrationIngredients):
        self.instance.prepRunConfig = mock.Mock()
        self.instance.prepPixelGroup = mock.Mock()
        self.instance.prepDetectorPeaks = mock.Mock()

        res = self.instance.prepDiffractionCalibrationIngredients(self.ingredients)

        assert self.instance.prepRunConfig.called_once_with(self.ingredients.runNumber)
        assert self.instance.prepPixelGroup.called_once_with(self.ingredients)
        assert self.instance.prepDetectorPeaks.called_once_with(self.ingredients)
        assert DiffractionCalibrationIngredients.called_once_with(
            runConfig=self.instance.prepRunConfig.return_value,
            pixelGroup=self.instance.prepRunConfig.return_value,
            groupedPeakLists=self.instance.prepDetectorPeaks.return_value,
            convergenceThreshold=self.ingredients.convergenceThreshold,
            maxOffset=self.ingredients.maxOffset,
        )
        assert res == DiffractionCalibrationIngredients.return_value
