import unittest
from typing import List

from snapred.backend.api.RequestScheduler import RequestScheduler
from snapred.backend.dao.SNAPRequest import SNAPRequest


class TestRequestScheduler(unittest.TestCase):
    def setUp(self):
        self.instance = RequestScheduler()
        # make payloads consistent
        request1 = SNAPRequest(path="test", payload="1")
        request2 = SNAPRequest(path="test", payload="2")
        request3 = SNAPRequest(path="test", payload="3")
        self.requests = [request1, request2, request3]

    def test_request_scheduler(self):
        # Lambda that sorts by even or odd
        def isEvenOrOdd(requests: List[SNAPRequest]):
            isEvenList = []
            isOddList = []
            for request in requests:
                runNumber = int(request.payload)
                if runNumber % 2 == 0:
                    isEvenList.append(request)
                else:
                    isOddList.append(request)
            return {"isEven": isEvenList, "isOdd": isOddList}

        # Lambda that sorts by value threshold of 2
        def isLessThan2(requests: List[SNAPRequest]):
            isLessThan2List = []
            isGreaterThan2List = []
            for request in requests:
                runNumber = int(request.payload)
                if runNumber < 2:
                    isLessThan2List.append(request)
                else:
                    isGreaterThan2List.append(request)
            return {"LessThan2": isLessThan2List, "GreaterThan2": isGreaterThan2List}

        # Requests will be sorted on even/odd first, then if greater than 2
        result = self.instance.handle(self.requests, [isEvenOrOdd, isLessThan2])

        assert result["root"]["isEven"]["LessThan2"] == []
        assert result["root"]["isEven"]["GreaterThan2"][0].payload == "2"
        assert result["root"]["isOdd"]["LessThan2"][0].payload == "1"
        assert result["root"]["isOdd"]["GreaterThan2"][0].payload == "3"
