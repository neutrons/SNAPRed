from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.widget.Workflow import Workflow


class WorkflowBuilder:
    def __init__(self, parent=None):
        self.parent = parent
        self._workflow = None

    def addNode(self, continueAction, subview, name="Unnamed"):
        if self._workflow is None:
            self._workflow = WorkflowNodeModel(continueAction=continueAction, view=subview, nextModel=None, name=name)
        else:
            currentWorkflow = self._workflow
            while currentWorkflow.nextModel is not None:
                currentWorkflow = currentWorkflow.nextModel
            currentWorkflow.nextModel = WorkflowNodeModel(
                continueAction=continueAction, view=subview, nextModel=None, name=name
            )
        return self

    def build(self):
        return Workflow(self._workflow, self.parent)
