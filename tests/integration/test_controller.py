from snapred.backend.dao.SNAPRequest import SNAPRequest


def test_excuteRequest_noop():
    # import must be here or it will put things in a funny state and break other tests
    from snapred.backend.api.InterfaceController import InterfaceController

    expected_keys = [       # TODO: reduction was removed, re-add this later
        "config",
        "stateId",
        "calibration",
        "ingestion",
        "calibrant_sample",
        "api",
        "normalization",
        "reduceLiteData",
        "workspace",
        "metadata",
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
