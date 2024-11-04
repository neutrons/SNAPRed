import unittest
from unittest import mock

import pytest
from mantid.simpleapi import mtd
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import deleteWorkspaceNoThrow

from snapred.backend.dao.request.SimpleDiffCalRequest import SimpleDiffCalRequest
from snapred.backend.service.CalibrationService import CalibrationService

ThisService: str = "snapred.backend.service.CalibrationService"
PixelCalRx: str = ThisService + ".PixelDiffCalRecipe"
GroupCalRx: str = ThisService + ".GroupDiffCalRecipe"


"""
NOTE this is in fact a test of the CalibrationService.  It used to test the combined DiffractionCalibrationRecipe.
That recipe mostly handled coordinating the pixel and group segments of the diff cal process.  With the most
recent refactor, that task of coordinating has been passed to the CalibrationService.  However, to make review
more transparent, these tests are being temporarily housed in the same file.
"""


class TestDiffractionCalibrationRecipe(unittest.TestCase):
    def setUp(self):
        self.syntheticInputs = SyntheticData()
        self.fakeIngredients = self.syntheticInputs.ingredients

        self.fakeRawData = "_test_diffcal_rx"
        self.fakeGroupingWorkspace = "_test_diffcal_rx_grouping"
        self.fakeDiagnosticWorkspace = "_test_diffcal_rx_diagnostic"
        self.fakeOutputWorkspace = "_test_diffcal_rx_dsp_output"
        self.fakeTableWorkspace = "_test_diffcal_rx_table"
        self.fakeMaskWorkspace = "_test_diffcal_rx_mask"
        self.syntheticInputs.generateWorkspaces(self.fakeRawData, self.fakeGroupingWorkspace, self.fakeMaskWorkspace)

        self.groceryList = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "outputWorkspace": self.fakeOutputWorkspace,
            "diagnosticWorkspace": self.fakeDiagnosticWorkspace,
            "calibrationTable": self.fakeTableWorkspace,
            "maskWorkspace": self.fakeMaskWorkspace,
        }
        self.service = CalibrationService()
        self.request = SimpleDiffCalRequest(
            ingredients=self.fakeIngredients,
            groceries=self.groceryList,
            skipPixelCalibration=False,
        )

    def tearDown(self) -> None:
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for ws in workspaces:
            deleteWorkspaceNoThrow(ws)
        return super().tearDown

    @mock.patch(PixelCalRx)
    @mock.patch(GroupCalRx)
    def test_execute_successful(self, mockGroupRx, mockPixelRx):
        # produce 4, 2, 1, 0.5
        mockPixelRx.return_value.cook.return_value = mock.Mock(
            result=True,
            medianOffsets=mock.sentinel.offsets,
        )
        mockGroupRx.return_value.cook.return_value = mock.Mock(
            result=True,
            diagnosticWorkspace=mock.sentinel.group,
            outputWorkspace=mock.sentinel.group,
            calibrationTable=mock.sentinel.group,
            maskWorkspace=mock.sentinel.group,
        )
        self.service.groceryService.getWorkspaceForName = mock.Mock(return_value=mtd[self.fakeMaskWorkspace])
        result = self.service.diffractionCalibrationWithIngredients(self.request)
        assert result["result"]
        assert result["steps"] == mock.sentinel.offsets
        assert result["calibrationTable"] == mock.sentinel.group
        assert result["outputWorkspace"] == mock.sentinel.group
        assert result["maskWorkspace"] == mock.sentinel.group

    @mock.patch(PixelCalRx)
    def test_execute_unsuccessful_pixel_cal(self, mockPixelRx):
        mockPixelRx.return_value.cook.return_value = mock.Mock(result=False)
        self.service.groceryService.getWorkspaceForName = mock.Mock(return_value=mtd[self.fakeMaskWorkspace])
        with pytest.raises(RuntimeError) as e:
            self.service.diffractionCalibrationWithIngredients(self.request)
        assert str(e.value) == "Pixel Calibration failed"

    @mock.patch(PixelCalRx)
    @mock.patch(GroupCalRx)
    def test_execute_unsuccessful_group_cal(self, mockGroupRx, mockPixelRx):
        mockPixelRx.return_value.cook.return_value = mock.Mock(result=True, medianOffsets=[0])
        mockGroupRx.return_value.cook.return_value = mock.Mock(result=False)
        self.service.groceryService.getWorkspaceForName = mock.Mock(return_value=mtd[self.fakeMaskWorkspace])
        with pytest.raises(RuntimeError) as e:
            self.service.diffractionCalibrationWithIngredients(self.request)
        assert str(e.value) == "Group Calibration failed"

    def test_execute_without_mocks(self):
        """
        Run the recipe with no mocks, to ensure both calculations can work.
        """

        rawWS = "_test_diffcal_rx_data"
        groupingWS = "_test_diffcal_grouping"
        maskWS = "_test_diffcal_mask"
        self.syntheticInputs.generateWorkspaces(rawWS, groupingWS, maskWS)

        self.groceryList["inputWorkspace"] = rawWS
        self.groceryList["groupingWorkspace"] = groupingWS
        self.groceryList["maskWorkspace"] = maskWS
        try:
            res = self.service.diffractionCalibrationWithIngredients(self.request)
        except ValueError:
            print(res)
        assert res["result"]

        assert res["maskWorkspace"]
        mask = mtd[res["maskWorkspace"]]
        assert mask.getNumberMasked() == 0
        assert res["steps"][-1] <= self.fakeIngredients.convergenceThreshold
