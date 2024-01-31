from snapred.ui.presenter.PlotTestPresenter import PlotTestPresenter
from snapred.ui.view.PlotTestView import PlotTestView


class PlotTest:
    def __init__(self, parent=None):
        self._view = PlotTestView(parent)
        self._presenter = PlotTestPresenter(self._view)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget
