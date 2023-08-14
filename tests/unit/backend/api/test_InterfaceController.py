import unittest.mock as mock

# Mock out of scope modules before importing InterfaceController
with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.service": mock.Mock(),
        "snapred.backend.service.ServiceFactory": mock.Mock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.api.InterfaceController import InterfaceController  # noqa: E402

    def mockedSuccessfulInterfaceController():
        """Mock InterfaceController"""
        interfaceController = InterfaceController()
        interfaceController.serviceFactory = mock.Mock()
        # when serviceFactory.getService is called with value 'Test Service', return a mock service
        mockService = mock.Mock()
        mockService.orchestrateRecipe.return_value = {"result": "Success!"}

        interfaceController.serviceFactory.getService.side_effect = (
            lambda x: mockService if x == "Test Service" else None
        )
        return interfaceController

    def test_executeRequest_successful():
        """Test executeRequest with a successful service"""
        interfaceController = mockedSuccessfulInterfaceController()
        reductionRequest = mock.Mock()
        reductionRequest.path = "Test Service"
        response = interfaceController.executeRequest(reductionRequest)
        assert response.code == 200
        assert response.message is None
        assert response.data["result"] == "Success!"

    def test_executeRequest_unsuccessful():
        """Test executeRequest with an unsuccessful service"""
        interfaceController = mockedSuccessfulInterfaceController()
        reductionRequest = mock.Mock()
        reductionRequest.path = "Non-existent Test Service"
        # mock orchestrateRecipe to raise an exception
        response = interfaceController.executeRequest(reductionRequest)
        assert response.code == 500
        assert response.message is not None
        assert response.data is None
