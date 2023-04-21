from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.SNAPResponse import SNAPResponse
from snapred.backend.log.logger import snapredLogger
from snapred.backend.service.ServiceFactory import ServiceFactory
from snapred.meta.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class InterfaceController:
    serviceFactory = ServiceFactory()

    def __init__(self):
        # make a singleton instance if one doesnt exist
        pass

    def executeRequest(self, request: SNAPRequest) -> SNAPResponse:
        result = None
        response = None
        # execute the recipe
        # return the result
        try:
            result = self.serviceFactory.getService(request.mode).orchestrateRecipe(request)

            response = SNAPResponse(responseCode=200, responseMessage=None, responseData=result)
        except Exception as e:  # noqa BLE001
            # handle exceptions, inform client if recoverable
            response = SNAPResponse(responseCode=500, responseMessage=str(e))
            logger.exception(str(e))

        logger.debug(response.json())
        return response
