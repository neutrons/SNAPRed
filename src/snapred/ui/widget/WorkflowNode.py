from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.presenter.WorkflowNodePresenter import WorkflowPresenter
from snapred.ui.view.WorkflowNodeView import WorkflowNodeView


def startWorkflow(action, subview):
    return WorkflowNodeModel(action=action, view=subview, nextModel=None)


def continueWorkflow(workflow, action, subview):
    currentWorkflow = workflow
    while currentWorkflow.nextModel is not None:
        currentWorkflow = workflow.nextModel

    currentWorkflow.nextModel = startWorkflow(action, subview)
    return workflow


def finalizeWorkflow(workflow, parent=None):
    return WorkflowNode(workflow, parent)


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
