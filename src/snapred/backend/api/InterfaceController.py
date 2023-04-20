from snapred.backend.service.ServiceFactory import ServiceFactory
from snapred.backend.dao.ReductionResponse import ReductionResponse
from snapred.backend.dao.ReductionRequest import ReductionRequest
from snapred.meta.Singleton import Singleton
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)

@Singleton
class InterfaceController:
    serviceFactory = ServiceFactory()

    def __init__(self):
        # make a singleton instance if one doesnt exist
        pass

    def executeRequest(self, reductionRequest: ReductionRequest) -> ReductionResponse:
        result = None
        response = None
        # execute the recipe
        # return the result
        try:
            result = self.serviceFactory \
            .getService(reductionRequest.mode) \
            .orchestrateRecipe(reductionRequest)
            
            response = ReductionResponse(responseCode=200, responseMessage=None, responseData=result)
        except Exception as e:
            # handle exceptions, inform client if recoverable
            response = ReductionResponse(responseCode=500, responseMessage=str(e))
            logger.exception(str(e))
        
        logger.debug(response.json())    
        return response

