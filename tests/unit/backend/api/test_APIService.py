import unittest.mock as mock

# Mock out of scope modules before importing DataExportService

with mock.patch.dict(
    "sys.modules",
    {"snapred.backend.data.DataExportService": mock.Mock(), "snapred.backend.data.DataFactoryService": mock.Mock()},
):
    from snapred.backend.dao.ReductionIngredients import ReductionIngredients
    from snapred.backend.service.ApiService import ApiService
    from snapred.backend.service.Service import Service
    from snapred.backend.service.ServiceDirectory import ServiceDirectory
    from snapred.meta.Config import Resource

    class MockService(Service):
        _name = "mockService"

        def __init__(self):
            super().__init__()
            self.registerPath("", self.testMethod)
            return

        def name(self):
            return self._name

        def testMethod(self, reductionIngredients: ReductionIngredients):
            return reductionIngredients

    def test_getValidPaths():
        apiService = ApiService()
        serviceDirectory = ServiceDirectory()
        mockService = MockService()
        serviceDirectory.registerService(mockService)
        validPaths = apiService.getValidPaths()
        with Resource.open("/outputs/APIServicePaths.json", "r") as f:
            import json

            actualValidPaths = json.load(f)
        assert validPaths == actualValidPaths
