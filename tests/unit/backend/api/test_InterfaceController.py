import unittest.mock as mock

import pytest

# Mock out of scope modules before importing InterfaceController
mock.patch("snapred.backend.service")
mock.patch("snapred.backend.service.ServiceFactory")
mock.patch("snapred.backend.log")
mock.patch("snapred.backend.log.logger")

from snapred.backend.api.InterfaceController import InterfaceController  # noqa: E402


def teardown():
    """Teardown after all tests"""
    mock.patch.stopall()


@pytest.fixture(autouse=True)
def setup_teardown():  # noqa: PT004
    """Setup before each test, teardown after each test"""
    yield
    teardown()


def mockedSuccessfulInterfaceController():
    """Mock InterfaceController"""
    interfaceController = InterfaceController()
    interfaceController.serviceFactory = mock.Mock()
    # when serviceFactory.getService is called with value 'Test Service', return a mock service
    mockService = mock.Mock()
    mockService.orchestrateRecipe.return_value = {"result": "Success!"}

    interfaceController.serviceFactory.getService.side_effect = lambda x: mockService if x == "Test Service" else None
    return interfaceController


def test_executeRequest_successful():
    """Test executeRequest with a successful service"""
    interfaceController = mockedSuccessfulInterfaceController()
    reductionRequest = mock.Mock()
    reductionRequest.path = "Test Service"
    response = interfaceController.executeRequest(reductionRequest)
    assert response.responseCode == 200
    assert response.responseMessage is None
    assert response.responseData["result"] == "Success!"


def test_executeRequest_unsuccessful():
    """Test executeRequest with an unsuccessful service"""
    interfaceController = mockedSuccessfulInterfaceController()
    reductionRequest = mock.Mock()
    reductionRequest.path = "Non-existent Test Service"
    # mock orchestrateRecipe to raise an exception
    response = interfaceController.executeRequest(reductionRequest)
    assert response.responseCode == 500
    assert response.responseMessage is not None
    assert response.responseData is None
