import unittest.mock as mock

import pytest
from snapred.backend.error.RecoverableException import RecoverableException

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

    def mockedSuccessfulInterfaceController(raiseRecoverable=False):
        """Mock InterfaceController"""
        interfaceController = InterfaceController()
        interfaceController.serviceFactory = mock.Mock()
        interfaceController.getWarnings = mock.Mock()
        interfaceController.getWarnings.return_value = None

        def orchestrateRecipe_side_effect(request):  # noqa: ARG001
            if raiseRecoverable:
                raise RecoverableException(
                    exception=AttributeError("'NoneType' object has no attribute 'instrumentState'"),
                    errorMsg="AttributeError: 'NoneType' object has no attribute 'instrumentState'",
                )
            else:
                return {"result": "Success!"}

        mockService = mock.Mock()
        mockService.orchestrateRecipe.side_effect = orchestrateRecipe_side_effect

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

    def test_executeRequest_recoverable():
        """Test executeRequest with a recoverable service"""
        interfaceController = mockedSuccessfulInterfaceController(raiseRecoverable=True)
        stateCheckRequest = mock.Mock()
        stateCheckRequest.path = "Test Service"

        response = interfaceController.executeRequest(stateCheckRequest)

        assert response.code == 400
        assert response.message == "state"
        assert response.data is None

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
