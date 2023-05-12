from snapred.ui.presenter.TestPanelPresenter import TestPanelPresenter
from snapred.ui.view.TestPanelView import TestPanelView


class TestPanel:
    def __init__(self, parent=None):
        self._view = TestPanelView(parent)
        self._presenter = TestPanelPresenter(self._view)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget
