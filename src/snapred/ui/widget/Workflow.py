from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.presenter.WorkflowPresenter import WorkflowPresenter
from snapred.ui.view.WorkflowView import WorkflowView


class Workflow:
    def __init__(self, model: WorkflowNodeModel, parent=None):
        # default loading subview
        self._presenter = WorkflowPresenter(model, parent)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget

    def show(self):
        self._presenter.show()
