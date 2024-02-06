from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.widget.Workflow import Workflow


class WorkflowBuilder:
    def __init__(self, cancelLambda=None, parent=None):
        self.parent = parent
        self._cancelLambda = cancelLambda
        self._workflow = None

    def addNode(self, continueAction, subview, name="Unnamed", required=True):
        if self._workflow is None:
            self._workflow = WorkflowNodeModel(
                continueAction=continueAction, view=subview, nextModel=None, name=name, required=required
            )
        else:
            currentWorkflow = self._workflow
            while currentWorkflow.nextModel is not None:
                currentWorkflow = currentWorkflow.nextModel
            currentWorkflow.nextModel = WorkflowNodeModel(
                continueAction=continueAction, view=subview, nextModel=None, name=name, required=required
            )
        return self

    def build(self):
        return Workflow(self._workflow, self._cancelLambda, self.parent)
