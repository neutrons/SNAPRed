import json
import unittest.mock as mock

from snapred.backend.api.RequestScheduler import RequestScheduler
from snapred.backend.dao.SNAPResponse import ResponseCode
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
        assert response.code == ResponseCode.OK
        assert response.message is None
        assert response.data["result"] == "Success!"

    def test_executeRequest_recoverable():
        """Test executeRequest with a recoverable service"""
        interfaceController = mockedSuccessfulInterfaceController(raiseRecoverable=True)
        stateCheckRequest = mock.Mock()
        stateCheckRequest.path = "Test Service"
        stateCheckRequest.payload = json.dumps({"runNumber": "12345", "useLiteMode": "True"})

        response = interfaceController.executeRequest(stateCheckRequest)

        assert response.code == ResponseCode.RECOVERABLE
        assert "state" in response.message
        assert response.data is None

    def test_executeRequest_unsuccessful():
        """Test executeRequest with an unsuccessful service"""
        interfaceController = mockedSuccessfulInterfaceController()
        reductionRequest = mock.Mock()
        reductionRequest.path = "Non-existent Test Service"

        response = interfaceController.executeRequest(reductionRequest)

        assert response.code == ResponseCode.ERROR
        assert response.message is not None
        assert response.data is None

    @mock.patch.object(RequestScheduler, "handle")
    def test_executeBatchRequests_successful(mockRequestScheduler):
        """Test executeBatchRequest with good requests"""
        interfaceController = mockedSuccessfulInterfaceController()
        reductionRequest = mock.Mock()
        reductionRequest.path = "Test Service"
        mockRequestScheduler.return_value = [reductionRequest, reductionRequest]
        responses = interfaceController.executeBatchRequests([reductionRequest, reductionRequest])
        assert responses[0].code == ResponseCode.OK
        assert responses[0].message is None
        assert responses[0].data["result"] == "Success!"
        assert len(responses) == 2

    def test_executeBatchRequests_unsuccessful():
        """Test executeBatchRequest with bad requests"""
        interfaceController = mockedSuccessfulInterfaceController()
        reductionRequest = mock.Mock()
        reductionRequest.path = "Test Service"
        # executeBatchRequest fails due to path mismatch
        badRequest = mock.Mock()
        badRequest.path = "Not same Path"
        responses = interfaceController.executeBatchRequests([reductionRequest, badRequest])
        assert responses is None
