from snapred.ui.model.model import LogTableModel
from snapred.ui.presenter.presenter import LogTablePresenter
from snapred.ui.view.view import LogTableView

class DummyWidget(object):
    """
    """
    def __init__(self, name, parent=None):
        model = LogTableModel()
        view = LogTableView(name, parent)
        self._presenter = LogTablePresenter(view, model)
        print(view.__dict__)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self.presenter.widget