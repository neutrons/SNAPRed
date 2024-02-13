from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.widget.Workflow import Workflow


class WorkflowBuilder:
    def __init__(self, cancelLambda=None, iterateLambda=None, parent=None):
        self.parent = parent
        self._cancelLambda = cancelLambda
        self._iterateLambda = iterateLambda
        self._workflow = None

    def addNode(self, continueAction, subview, name="Unnamed", required=True, iterate=False):
        if self._workflow is None:
            self._workflow = WorkflowNodeModel(
                continueAction=continueAction,
                view=subview,
                nextModel=None,
                name=name,
                required=required,
                iterate=iterate,
            )
        else:
            currentWorkflow = self._workflow
            while currentWorkflow.nextModel is not None:
                currentWorkflow = currentWorkflow.nextModel
            currentWorkflow.nextModel = WorkflowNodeModel(
                continueAction=continueAction,
                view=subview,
                nextModel=None,
                name=name,
                required=required,
                iterate=iterate,
            )
        return self

    def build(self):
        return Workflow(self._workflow, self._cancelLambda, self._iterateLambda, self.parent)
