from src.snapred.ui.model.LogTableModel import LogTableModel
from src.snapred.ui.presenter.LogTablePresenter import LogTablePresenter
from src.snapred.ui.view.LogTableView import LogTableView


class LogTable(object):
    """ """

    def __init__(self, name, parent=None):
        model = LogTableModel()
        view = LogTableView(name, parent)
        self._presenter = LogTablePresenter(view, model)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget
