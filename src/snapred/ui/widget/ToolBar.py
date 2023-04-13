from PyQt5.QtWidgets import QWidget
from snapred.ui.presenter.ToolBarPresenter import ToolBarPresenter
from snapred.ui.view.ToolBarView import ToolBarView


class ToolBar(QWidget):
    def __init__(self, parent=None):
        self._view = ToolBarView(parent)
        self._presenter = ToolBarPresenter(self._view)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget
