from snapred.backend.dao.SNAPRequest import SNAPRequest


# should probably use the DAO layer to some degree
class LogTableModel(object):
    def __init__(self):
        self.someVariable = SNAPRequest(mode="test", runs=[])

    def getRecipeConfig(self):
        return self.someVariable

    def addRecipeConfig(self, request: SNAPRequest):
        self.someVariable = request
