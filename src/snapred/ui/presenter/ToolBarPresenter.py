class ToolBarPresenter:
    def __init__(self, view):
        self._view = view

    @property
    def widget(self):
        return self.view
