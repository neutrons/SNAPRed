from snapred.ui.presenter.ActionPromptPresenter import ActionPromptPresenter
from snapred.ui.view.ActionPromptView import ActionPromptView


class ActionPrompt:
    def __init__(self, title, message, action, parent=None):
        self.title = title
        self.message = message
        self.action = action
        self.view = ActionPromptView(self.title, self.message, parent)
        self.presenter = ActionPromptPresenter(self.view, self.action)
        self.view.show()

    @property
    def widget(self):
        return self.view

    # A static "factory" method to facilitate testing.
    @staticmethod
    def prompt(title, message, action, parent=None):
        ActionPrompt(title, message, action, parent)
