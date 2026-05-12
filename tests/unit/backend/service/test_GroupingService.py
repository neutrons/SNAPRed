import unittest
import unittest.mock as mock

from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.service.GroupingService import GroupingService


class TestGroupingService(unittest.TestCase):
    def setUp(self):
        self.instance = GroupingService.__new__(GroupingService)
        self.instance.groceryService = mock.MagicMock()
        self.instance.mantidSnapper = mock.MagicMock()
        self.instance._straddleCache = {}

    # --- __init__ ---

    @mock.patch("snapred.backend.service.GroupingService.MantidSnapper")
    @mock.patch("snapred.backend.service.GroupingService.GroceryService")
    def test_init_default_groceryService(self, MockGroceryService, MockMantidSnapper):
        """When no groceryService is passed, a new GroceryService() is created."""
        instance = GroupingService.__new__(GroupingService)
        instance.__init__()
        MockGroceryService.assert_called_once()
        assert instance.groceryService is MockGroceryService.return_value
        MockMantidSnapper.assert_called_once_with(None, "snapred.backend.service.GroupingService")
        assert instance.mantidSnapper is MockMantidSnapper.return_value

    @mock.patch("snapred.backend.service.GroupingService.MantidSnapper")
    @mock.patch("snapred.backend.service.GroupingService.GroceryService")
    def test_init_custom_groceryService(self, MockGroceryService, MockMantidSnapper):
        """When a groceryService is passed, it is used directly."""
        custom_gs = mock.MagicMock()
        instance = GroupingService.__new__(GroupingService)
        instance.__init__(groceryService=custom_gs)
        MockGroceryService.assert_not_called()
        assert instance.groceryService is custom_gs
        MockMantidSnapper.assert_called_once_with(None, "snapred.backend.service.GroupingService")

    # --- isGroupNonStraddling ---

    def test_isGroupNonStraddling_returns_true_when_no_straddle(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback = mock.MagicMock()
        callback.get.return_value = []
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.return_value = callback

        assert self.instance.isGroupNonStraddling("Column", "12345", True) is True

    def test_isGroupNonStraddling_returns_false_when_straddling(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback = mock.MagicMock()
        callback.get.return_value = ["1", "2"]
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.return_value = callback

        assert self.instance.isGroupNonStraddling("All", "12345", True) is False

    def test_isGroupNonStraddling_caches_result(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback = mock.MagicMock()
        callback.get.return_value = []
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.return_value = callback

        self.instance.isGroupNonStraddling("Column", "12345", True)
        self.instance.isGroupNonStraddling("Column", "99999", False)

        # Only one call despite two invocations with same group name
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.assert_called_once()

    # --- filterCalibrationRecordsByNonStraddlingGroups ---

    def test_filterCalibrationRecords_keeps_valid(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback = mock.MagicMock()
        callback.get.return_value = []
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.return_value = callback

        record = mock.MagicMock(spec=CalibrationRecord)
        record.focusGroupCalibrationMetrics = mock.MagicMock(spec=FocusGroupMetric)
        record.focusGroupCalibrationMetrics.focusGroupName = "Column"

        result = self.instance.filterCalibrationRecordsByNonStraddlingGroups([record], "12345", True)
        assert result == [record]
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.assert_called_once_with(
            "Checking panel straddle", GroupingWorkspace="fake_ws"
        )

    def test_filterCalibrationRecords_excludes_straddling(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback = mock.MagicMock()
        callback.get.return_value = ["1", "2"]
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.return_value = callback

        record = mock.MagicMock(spec=CalibrationRecord)
        record.focusGroupCalibrationMetrics = mock.MagicMock(spec=FocusGroupMetric)
        record.focusGroupCalibrationMetrics.focusGroupName = "All"
        record.runNumber = "12345"
        record.version = 1

        result = self.instance.filterCalibrationRecordsByNonStraddlingGroups([record], "12345", True)
        assert result == []

    def test_filterCalibrationRecords_mixed(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback1 = mock.MagicMock()
        callback1.get.return_value = []
        callback2 = mock.MagicMock()
        callback2.get.return_value = ["1"]
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.side_effect = [callback1, callback2]

        record_good = mock.MagicMock(spec=CalibrationRecord)
        record_good.focusGroupCalibrationMetrics = mock.MagicMock(spec=FocusGroupMetric)
        record_good.focusGroupCalibrationMetrics.focusGroupName = "Column"

        record_bad = mock.MagicMock(spec=CalibrationRecord)
        record_bad.focusGroupCalibrationMetrics = mock.MagicMock(spec=FocusGroupMetric)
        record_bad.focusGroupCalibrationMetrics.focusGroupName = "All"
        record_bad.runNumber = "12345"
        record_bad.version = 2

        result = self.instance.filterCalibrationRecordsByNonStraddlingGroups([record_good, record_bad], "12345", True)
        assert result == [record_good]

    def test_filterCalibrationRecords_caches_per_groupname(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback = mock.MagicMock()
        callback.get.return_value = []
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.return_value = callback

        records = []
        for _ in range(3):
            r = mock.MagicMock(spec=CalibrationRecord)
            r.focusGroupCalibrationMetrics = mock.MagicMock(spec=FocusGroupMetric)
            r.focusGroupCalibrationMetrics.focusGroupName = "Column"
            records.append(r)

        result = self.instance.filterCalibrationRecordsByNonStraddlingGroups(records, "12345", True)
        assert len(result) == 3
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.assert_called_once()

    # --- filterNormalizationRecordsByNonStraddlingGroups ---

    def test_filterNormalizationRecords_keeps_valid(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback = mock.MagicMock()
        callback.get.return_value = []
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.return_value = callback

        record = mock.MagicMock(spec=NormalizationRecord)
        record.calculationParameters = mock.MagicMock()
        record.calculationParameters.name = "Column"

        result = self.instance.filterNormalizationRecordsByNonStraddlingGroups([record], "12345", True)
        assert result == [record]
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.assert_called_once_with(
            "Checking panel straddle", GroupingWorkspace="fake_ws"
        )

    def test_filterNormalizationRecords_excludes_straddling(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback = mock.MagicMock()
        callback.get.return_value = ["1", "2"]
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.return_value = callback

        record = mock.MagicMock(spec=NormalizationRecord)
        record.calculationParameters = mock.MagicMock()
        record.calculationParameters.name = "All"
        record.runNumber = "12345"
        record.version = 1

        result = self.instance.filterNormalizationRecordsByNonStraddlingGroups([record], "12345", True)
        assert result == []

    def test_filterNormalizationRecords_mixed(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback1 = mock.MagicMock()
        callback1.get.return_value = []
        callback2 = mock.MagicMock()
        callback2.get.return_value = ["1"]
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.side_effect = [callback1, callback2]

        record_good = mock.MagicMock(spec=NormalizationRecord)
        record_good.calculationParameters = mock.MagicMock()
        record_good.calculationParameters.name = "Column"

        record_bad = mock.MagicMock(spec=NormalizationRecord)
        record_bad.calculationParameters = mock.MagicMock()
        record_bad.calculationParameters.name = "All"
        record_bad.runNumber = "12345"
        record_bad.version = 2

        result = self.instance.filterNormalizationRecordsByNonStraddlingGroups([record_good, record_bad], "12345", True)
        assert result == [record_good]

    def test_filterNormalizationRecords_caches_per_groupname(self):
        self.instance.groceryService.fetchGroupingDefinition.return_value = {"workspace": "fake_ws"}
        callback = mock.MagicMock()
        callback.get.return_value = []
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.return_value = callback

        records = []
        for _ in range(3):
            r = mock.MagicMock(spec=NormalizationRecord)
            r.calculationParameters = mock.MagicMock()
            r.calculationParameters.name = "Column"
            records.append(r)

        result = self.instance.filterNormalizationRecordsByNonStraddlingGroups(records, "12345", True)
        assert len(result) == 3
        self.instance.mantidSnapper.DetectorPanelStraddleCheck.assert_called_once()
