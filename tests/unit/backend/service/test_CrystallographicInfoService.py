import unittest
from unittest.mock import Mock, patch

import pytest
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService

thisService = "snapred.backend.service.CrystallographicInfoService."


class TestXtalService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.instance = CrystallographicInfoService()
        super().setUpClass()

    def test_name(self):
        assert "ingestion" == self.instance.name()

    @patch(thisService + "CrystallographicInfoRecipe")
    def test_ingest_good(self, xtalRx):
        xtalRx.return_value.executeRecipe.return_value = {"good": "yup"}
        cifPath = "apples"
        data = self.instance.ingest(cifPath)
        assert xtalRx.called_once
        assert xtalRx.return_value.executeRecipe.called_once_with(cifPath)
        assert data == xtalRx.return_value.executeRecipe.return_value

    @patch(thisService + "CrystallographicInfoRecipe")
    def test_ingest_bad(self, xtalRx):
        xtalRx.side_effect = RuntimeError("nope!")
        cifPath = "bananas"
        with pytest.raises(RuntimeError) as e:
            self.instance.ingest(cifPath)
        assert "nope!" == str(e.value)
