import inspect

from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
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
            # ServiceFactory doesn't always return an instance so it needs to be fixed here
            service = self.serviceFactory.getService(request.path)
            if inspect.isclass(service):
                try:
                    service = service()
                except Exception as e:  # noqa BLE001
                    raise RuntimeError("Failed to create service") from e
            # run the recipe
            result = service.orchestrateRecipe(request)
            # convert the response into object to communicate with
            response = SNAPResponse(responseCode=200, responseMessage=None, responseData=result)
        except Exception as e:  # noqa BLE001
            # handle exceptions, inform client if recoverable
            self.logger.exception("Failed to call service")
            response = SNAPResponse(responseCode=500, responseMessage=str(e))

        self.logger.debug(response.json())
        return response
