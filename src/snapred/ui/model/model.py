from snapred.backend.dao.RecipeConfig import RecipeConfig

# should probably use the DAO layer to some degree
class LogTableModel(object):

    def __init__(self):
        self.someVariable = RecipeConfig(mode="test", runs=[])

    def getRecipeConfig(self):
        return self.someVariable

    def setRecipeConfig(self, recipeConfig):
        self.someVariable = recipeConfig