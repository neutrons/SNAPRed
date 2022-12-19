from PyQt5 import QtGui, QtCore
from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.RecipeConfig import RecipeConfig
from snapred.backend.dao.RunConfig import RunConfig


class LogTablePresenter(object):

    interfaceController = InterfaceController()

    def __init__(self, view, model):
        self.view = view
        self.model = model

        self.view.on_button_clicked(self.handle_button_clicked)

    @property
    def widget(self):
        return self.view
    
    def show(self):
      self.view.show()

    def handle_button_clicked(self):
        recipeConfig = RecipeConfig(mode="Config Lookup", runs=[RunConfig("000001")])
        reductionConfig = self.interfaceController.executeRecipe(recipeConfig)
        
        self.model.setRecipeConfig(reductionConfig)
        self.view.addRecipeConfig(self.model.getRecipeConfig())