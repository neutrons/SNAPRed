class ToolBarPresenter:
    def __init__(self, view):
        self._view = view

    @property
    def widget(self):
        return self._view

    def windowStateChange(self, windowState):
        self.widget.windowStateChanged(windowState)

    def resizeEvent(self, event):
        self.widget.resizeEvent(event)