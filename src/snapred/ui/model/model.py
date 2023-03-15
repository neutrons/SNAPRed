from snapred.backend.dao.ReductionRequest import ReductionRequest

# should probably use the DAO layer to some degree
class LogTableModel(object):

    def __init__(self):
        self.someVariable = ReductionRequest(mode="test", runs=[])

    def getRecipeConfig(self):
        return self.someVariable

    def addRecipeConfig(self, reductionRequest):
        self.someVariable = reductionRequest