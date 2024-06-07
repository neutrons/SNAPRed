from typing import Callable, Dict, List

from snapred.backend.dao.SNAPRequest import SNAPRequest

# Type define which is a callable function with a List of SNAPRequests as input,
# and a Dict of str keys and List of SNAPRequests values as expected output.
GroupingLambda = Callable[[List[SNAPRequest]], Dict[str, List[SNAPRequest]]]


class RequestScheduler:
    def _groupRequests(self, requests: Dict, groupingLambda: GroupingLambda):
        for k, v in requests.copy().items():
            # If input has been sorted multiple times, recursively call the function until base list is reached
            if isinstance(v, Dict):
                v = self._groupRequests(v, groupingLambda)
                requests[k] = v
            elif isinstance(v, List):
                requests[k] = groupingLambda(v)
            else:
                raise ValueError(f"Submitted invalid structure, value is not list or dict for key pair {k}:{str(v)}")
        return requests

    def handle(self, requests: List[SNAPRequest], groupingLambdas: List[GroupingLambda]):
        sortedRequests = {"root": requests}
        for groupingLambda in groupingLambdas:
            sortedRequests = self._groupRequests(sortedRequests, groupingLambda)
        return sortedRequests
