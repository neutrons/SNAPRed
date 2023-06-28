from snapred.backend.algorithm.CustomStripPeaksAlgorithm import CustomStripPeaksAlgorithm
from snapred.backend.recipe.GenericRecipe import GenericRecipe
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class CustomStripPeaksRecipe(GenericRecipe[CustomStripPeaksAlgorithm]):
    def __init__(self):
        super().__init__()
        return
