from snapred.backend.service.ServiceFactory import ServiceFactory
from snapred.backend.dao.ReductionResponse import ReductionResponse
from snapred.meta.Singleton import Singleton


@Singleton
class InterfaceController:
    serviceFactory = ServiceFactory()

    def __init__(self):
        # make a singleton instance if one doesnt exist
        pass

    def executeRequest(self, reductionRequest):
        result = None
        # execute the recipe
        # return the result
        # try:
        print(self.serviceFactory.getServiceNames())
        result = self.serviceFactory \
        .getService(reductionRequest.mode) \
        .orchestrateRecipe(reductionRequest)
        # except Exception as e:
        #     # handle exceptions, inform client if recoverable
        #     return {"exception": str(e)}
        return ReductionResponse(responseCode=200, responseMessage=None, responseData=result)

