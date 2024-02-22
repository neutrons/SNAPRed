class ActionPromptPresenter:
    def __init__(self, view, action):
        self.view = view
        self.action = action
        self.view.onContinueButtonClicked(self.action)
