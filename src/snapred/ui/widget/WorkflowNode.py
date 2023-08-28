from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.presenter.WorkflowNodePresenter import WorkflowPresenter
from snapred.ui.view.WorkflowNodeView import WorkflowNodeView


class WorkflowNode:
    def __init__(self, model, parent=None):
        # default loading subview
        subview = model.view
        view = WorkflowNodeView(subview, parent)
        self._presenter = WorkflowPresenter(view, model)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget
