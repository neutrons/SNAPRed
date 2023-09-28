import inspect

from snapred.backend.dao import SNAPRequest, SNAPResponse
from snapred.backend.log.logger import snapredLogger
from snapred.backend.service.ServiceFactory import ServiceFactory
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class InterfaceController:
    serviceFactory = ServiceFactory()

    def __init__(self):
        # make a singleton instance if one doesnt exist
        self.logger = snapredLogger.getLogger(self.__class__.__name__)

    def executeRequest(self, request: SNAPRequest) -> SNAPResponse:
        # execute the recipe
        # return the result
        try:
            self.logger.debug(f"Request Received: {request.json()}")
            # leaving this a separate line makes stack traces make more sense
            service = self.serviceFactory.getService(request.path)
            # run the recipe
            result = service.orchestrateRecipe(request)
            # convert the response into object to communicate with
            response = SNAPResponse(code=200, message=None, data=result)
        except Exception as e:  # noqa BLE001
            # handle exceptions, inform client if recoverable
            self.logger.exception("Failed to call service")
            response = SNAPResponse(code=500, message=str(e))
            return response

        self.logger.debug(response.json())
        return response
