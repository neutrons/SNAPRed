
from snapred.meta.Config import Config
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService

import unittest
from unittest import mock
import pytest

thisService = "snapred.backend.service.CrystallographicInfoService."


class TestXtalService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.instance = CrystallographicInfoService()
        super().setUpClass()

    def test_name(self):
        assert "ingestion" == self.instance.name()

    @mock.patch(thisService + "CrystallographicInfoRecipe")
    def test_ingest_good(self, xtalRx):
        D_MIN = Config["constants.CrystallographicInfo.crystalDMin"]
        D_MAX = Config["constants.CrystallographicInfo.crystalDMax"]
        xtalRx.return_value.executeRecipe.return_value = {"good": "yup"}
        cifPath = "apples"
        data = self.instance.ingest(cifPath)
        xtalRx.assert_called_once()
        xtalRx.return_value.executeRecipe.assert_called_once_with(
            cifPath=cifPath, crystalDMin=D_MIN, crystalDMax=D_MAX
        )
        assert data == xtalRx.return_value.executeRecipe.return_value

    @mock.patch(thisService + "CrystallographicInfoRecipe")
    def test_ingest_bad(self, xtalRx):
        xtalRx.side_effect = RuntimeError("nope!")
        cifPath = "bananas"
        with pytest.raises(RuntimeError) as e:
            self.instance.ingest(cifPath)
        assert "nope!" == str(e.value)
