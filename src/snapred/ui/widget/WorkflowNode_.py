from snapred.ui.presenter.WorkflowNodePresenter import WorkflowNodePresenter
from snapred.ui.view.WorkflowNodeView import WorkflowNodeView


class WorkflowNode:
    def __init__(self, model, parent=None):
        # default loading subview
        subview = model.view
        view = WorkflowNodeView(subview, parent)
        self._presenter = WorkflowNodePresenter(view, model)

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget
