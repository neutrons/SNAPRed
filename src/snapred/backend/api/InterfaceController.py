from snapred.backend.service.ServiceFactory import ServiceFactory
from snapred.meta.Singleton import Singleton


@Singleton
class InterfaceController:
    serviceFactory = ServiceFactory()

    def __init__(self):
        # make a singleton instance if one doesnt exist
        pass

    def executeRecipe(self, recipeConfig):
        # execute the recipe
        # return the result
        # try:
        print(self.serviceFactory.getServiceNames())
        return self.serviceFactory \
        .getService(recipeConfig.mode) \
        .executeRecipe(recipeConfig)
        # except Exception as e:
        #     # handle exceptions, inform client if recoverable
        #     return {"exception": str(e)}
