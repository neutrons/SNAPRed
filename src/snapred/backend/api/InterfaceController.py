import inspect

from snapred.backend.dao import SNAPRequest, SNAPResponse
from snapred.backend.error import RecoverableException
from snapred.backend.log.logger import snapredLogger
from snapred.backend.service.ServiceFactory import ServiceFactory
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class InterfaceController:
    serviceFactory = ServiceFactory()

    def __init__(self):
        # make a singleton instance if one doesnt exist
        self.logger = snapredLogger.getLogger(self.__class__.__name__)

    def getWarnings(self):
        return "\n".join(snapredLogger.getWarnings())

    def executeRequest(self, request: SNAPRequest) -> SNAPResponse:
        # execute the recipe
        # return the result
        try:
            self.logger.debug(f"Request Received: {request.json()}")
            snapredLogger.clearWarnings()

            service = self.serviceFactory.getService(request.path)
            result = service.orchestrateRecipe(request)

            message = None
            if snapredLogger.hasWarnings():
                message = self.getWarnings()
            response = SNAPResponse(code=200, message=message, data=result)

        except RecoverableException as e:
            self.logger.error(f"Recoverable error occurred: {e.message}")
            response = SNAPResponse(code=400, message="state", data=None)

        except Exception as e:  # noqa BLE001
            # handle exceptions, inform client if recoverable
            self.logger.exception("Failed to call service")
            response = SNAPResponse(code=500, message=str(e))

        finally:
            snapredLogger.clearWarnings()

        self.logger.debug(response.json())
        return response
