import pytest
from snapred.backend.dao.SNAPRequest import SNAPRequest


@pytest.mark.integration()
def test_executeRequest_noop():
    # import must be here or it will put things in a funny state and break other tests
    from snapred.backend.api.InterfaceController import InterfaceController

    expected_keys = [
        "api",
        "calibrant_sample",
        "calibration",
        "config",
        "ingestion",
        "metadata",
        "normalization",
        "reduceLiteData",
        "reduction",
        "stateId",
        "workspace",
    ]
    expected_keys.sort()

    interfaceController = InterfaceController()

    request = SNAPRequest(path="api")
    response = interfaceController.executeRequest(request=request)

    # verify response code
    assert response.code == 200

    # verify the expected keys
    apiDict = response.data
    keys = list(apiDict.keys())
    keys.sort()
    assert keys == expected_keys
