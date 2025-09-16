from typing import List

from snapred.backend.api.HookManager import HookManager
from snapred.backend.api.PubSubManager import PubSubManager
from snapred.backend.api.RequestScheduler import RequestScheduler
from snapred.backend.dao import SNAPRequest, SNAPResponse
from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.service.ServiceFactory import ServiceFactory
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class InterfaceController:
    """

    InterfaceController serves as the central controller for handling SNAPRequests and generating SNAPResponses.
    It utilizes the ServiceFactory to delegate the request to the appropriate service and handles both normal and
    recoverable exceptions to ensure robustness in request processing. This controller is designed as a Singleton
    to maintain a single instance throughout the application's lifecycle, ensuring consistent state and behavior.

    """

    serviceFactory = ServiceFactory()
    hookManager = HookManager()
    pubSubManager = PubSubManager()

    def __init__(self):
        # make a singleton instance if one doesnt exist
        self.logger = snapredLogger.getLogger(self.__class__.__name__)

    def getWarnings(self):
        return "\n".join(snapredLogger.getWarnings())

    def executeRequest(self, request: SNAPRequest) -> SNAPResponse:
        # execute the request
        # return the result
        try:
            self.logger.debug(f"Request Received: {request.json()}")
            snapredLogger.clearWarnings()
            self.hookManager.register(request.hooks)
            service = self.serviceFactory.getService(request.path)
            result = service.orchestrateRecipe(request)

            self.hookManager.validateAllHooksExecuted()

            message = None
            if snapredLogger.hasWarnings():
                message = self.getWarnings()
            response = SNAPResponse(code=ResponseCode.OK, message=message, data=result)

        except RecoverableException as e:
            self.logger.error(f"Recoverable error occurred: {str(e)}")
            response = SNAPResponse(code=ResponseCode.RECOVERABLE, message=e.model.json())
        except LiveDataState as e:
            self.logger.error(
                f"live-data state change: {e.model.transition}: "
                + f"{e.model.endRunNumber} <- {e.model.startRunNumber}"
            )
            response = SNAPResponse(code=ResponseCode.LIVE_DATA_STATE, message=e.model.json())
        except ContinueWarning as e:
            self.logger.error(f"Continue warning occurred: {str(e)}")
            response = SNAPResponse(code=ResponseCode.CONTINUE_WARNING, message=e.model.json())
        except Exception as e:  # noqa BLE001
            # handle exceptions, inform client if recoverable
            self.logger.exception("Failed to call service")
            response = SNAPResponse(code=ResponseCode.ERROR, message=str(e))

        finally:
            snapredLogger.clearWarnings()
            self.hookManager.reset()

        self.logger.debug(response.json())
        return response

    def executeBatchRequests(self, requests: List[SNAPRequest]) -> List[SNAPResponse]:
        # verify that all requests have the same path
        for request in requests:
            if not requests[0].path == request.path:
                self.logger.error("Mismatch of paths in list of requests")
                return None

        # reorder the list of requests
        service = self.serviceFactory.getService(requests[0].path)
        scheduler = RequestScheduler()
        groupings = service.getGroupings("")
        orderedRequests = scheduler.handle(requests, groupings)

        # execute the ordered list of requests
        responses = []
        for request in orderedRequests:
            responses.append(self.executeRequest(request))

        return responses

    def subscribe(self, eventType: str, callback):
        self.pubSubManager.subscribe(eventType, callback)
