from snapred.ui.presenter.ProgressPresenter import ProgressPresenter
from snapred.ui.view.ProgressView import ProgressView


class ProgressWidget:
    def __init__(self):
        self.view = ProgressView()
        self.presenter = ProgressPresenter(self.view)
