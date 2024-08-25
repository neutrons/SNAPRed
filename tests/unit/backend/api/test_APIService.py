import json
import sys
import unittest.mock as mock
from typing import List

from pydantic import BaseModel


class TestAPIService:
    """
    THIS initialization mocks the imported modules AT the time the test is actually RUN.
    NOT AT the time the MODULE LOAD is triggered during the pytest-setup phase.
    The effect of the latter is difficult to predict when considering modules which may
    contain shallow references to other modules.
    """

    @mock.patch.dict(sys.modules)
    def test_getValidPaths(self):
        sys.modules.pop("snapred.backend.service.ApiService", None)
        sys.modules.pop("snapred.backend.service.Service", None)
        sys.modules.pop("snapred.backend.service.ServiceDirectory", None)
        from snapred.backend.service.ApiService import ApiService
        from snapred.backend.service.Service import Service
        from snapred.backend.service.ServiceDirectory import ServiceDirectory
        from snapred.meta.Config import Resource

        class MockObject(BaseModel):
            m_list: List[float]
            m_float: float
            m_int: int
            m_string: str

        class MockService(Service):
            _name = "mockService"

            def __init__(self):
                super().__init__()
                self.registerPath("", self.testMethod)
                return

            def name(self):
                return self._name

            def testMethod(self, mockObject: MockObject):
                return mockObject

        apiService = ApiService()
        serviceDirectory = ServiceDirectory()
        mockService = MockService()
        serviceDirectory.registerService(mockService)
        validPaths = apiService.getValidPaths()
        with Resource.open("/outputs/APIServicePaths.json", "r") as f:
            actualValidPaths = json.load(f)
            assert validPaths == actualValidPaths
