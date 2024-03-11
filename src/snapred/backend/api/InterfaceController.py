import inspect
import json

from snapred.backend.dao import SNAPRequest, SNAPResponse
from snapred.backend.dao.request.InitializeStateHandler import InitializeStateHandler
from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.backend.error.RecoverableException import RecoverableException
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
            response = SNAPResponse(code=ResponseCode.OK, message=message, data=result)

        except RecoverableException as e:
            self.logger.error(f"Recoverable error occurred: {str(e)}")
            payloadDict = json.loads(request.payload)
            runNumber = payloadDict["runNumber"]
            if runNumber:
                InitializeStateHandler.runId = runNumber
            response = SNAPResponse(code=ResponseCode.RECOVERABLE, message="state")

        except Exception as e:  # noqa BLE001
            # handle exceptions, inform client if recoverable
            self.logger.exception("Failed to call service")
            response = SNAPResponse(code=ResponseCode.ERROR, message=str(e))

        finally:
            snapredLogger.clearWarnings()

        self.logger.debug(response.json())
        return response
