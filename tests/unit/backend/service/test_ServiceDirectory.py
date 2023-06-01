import unittest.mock as mock

# Mock out of scope modules before importing DataExportService

with mock.patch.dict(
    "sys.modules",
    {"snapred.backend.data.DataExportService": mock.Mock(), "snapred.backend.data.DataFactoryService": mock.Mock()},
):
    from snapred.backend.service.ServiceDirectory import ServiceDirectory

    def test_serviceDirectory():
        serviceDirectory = ServiceDirectory()
        mockService = mock.Mock()
        mockService.name.return_value = "mockService"
        serviceDirectory.registerService(mockService)
        assert serviceDirectory["mockService"] == mockService
        assert serviceDirectory.get("mockService", None) == mockService
        assert serviceDirectory.asDict()["mockService"] == mockService
        assert serviceDirectory.keys() == serviceDirectory.asDict().keys()
