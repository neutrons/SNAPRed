import unittest.mock as mock

# Mock out of scope modules before importing DataExportService

localMock = mock.Mock()

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.data.DataExportService": mock.Mock(),
        "snapred.backend.data.DataFactoryService": mock.Mock(),
        "snapred.backend.recipe.CalibrationReductionRecipe": mock.Mock(),
        "snapred.backend.dao.PixelGroupingIngredients": mock.MagicMock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord  # noqa: E402
    from snapred.backend.dao.ReductionIngredients import ReductionIngredients  # noqa: E402
    from snapred.backend.dao.RunConfig import RunConfig  # noqa: E402
    from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import (
        PixelGroupingParametersCalculationRecipe,  # noqa: E402
    )
    from snapred.backend.service.CalibrationService import CalibrationService  # noqa: E402
    from snapred.meta.Config import Resource  # noqa: E402

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/calibration/input.json", "r") as f:
            return ReductionIngredients.parse_raw(f.read())

    # test export calibration
    def test_exportCalibrationIndex():
        dataExportService = CalibrationService()
        dataExportService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        dataExportService.dataExportService.exportCalibrationIndexEntry.return_value = "expected"
        dataExportService.saveCalibrationToIndex(CalibrationIndexEntry(runNumber="1", comments="", author=""))
        assert dataExportService.dataExportService.exportCalibrationIndexEntry.called
        savedEntry = dataExportService.dataExportService.exportCalibrationIndexEntry.call_args.args[0]
        assert savedEntry.appliesTo == ">1"
        assert savedEntry.timestamp is not None

    def test_save():
        dataExportService = CalibrationService()
        dataExportService.dataExportService.exportCalibrationRecord = mock.Mock()
        dataExportService.dataExportService.exportCalibrationRecord.return_value = CalibrationRecord(
            parameters=readReductionIngredientsFromFile(), version="1"
        )
        dataExportService.dataFactoryService.getReductionIngredients = mock.Mock()
        dataExportService.dataFactoryService.getReductionIngredients.return_value = readReductionIngredientsFromFile()
        dataExportService.save(CalibrationIndexEntry(runNumber="1", comments="", author=""))
        assert dataExportService.dataExportService.exportCalibrationRecord.called
        savedEntry = dataExportService.dataExportService.exportCalibrationRecord.call_args.args[0]
        assert savedEntry.parameters is not None

    # test calculate pixel grouping parameters
    def test_calculatePixelGroupingParameters():
        calibrationService = CalibrationService()
        runs = [RunConfig(runNumber="1")]
        groupingFile = mock.Mock()
        setattr(PixelGroupingParametersCalculationRecipe, "executeRecipe", localMock)
        localMock.return_value = mock.MagicMock()
        calibrationService.calculatePixelGroupingParameters(runs, groupingFile)
        assert localMock.called
