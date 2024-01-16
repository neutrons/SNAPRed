from unittest.mock import patch

from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService


@patch("snapred.backend.service.CrystallographicInfoService.CrystallographicInfoRecipe")
def test_CrystallographicInfoService(mockRecipe):
    mockRecipe = mockRecipe.return_value
    service = CrystallographicInfoService()
    assert service.name() == "ingestion"

    result = service.ingest("cifPath", 1.0)
    mockRecipe.executeRecipe.assert_called_once_with("cifPath", dMin=1.0)
    assert result == mockRecipe.executeRecipe.return_value
