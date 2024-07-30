from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.widget.Workflow import Workflow


class WorkflowBuilder:
    def __init__(self, *, startLambda=None, iterateLambda=None, resetLambda=None, cancelLambda=None, parent=None):
        self.parent = parent
        self._startLambda = startLambda
        self._iterateLambda = iterateLambda
        self._resetLambda = resetLambda
        self._cancelLambda = cancelLambda
        self._workflow = None

    def addNode(
        self, continueAction, subview, name="Unnamed", required=True, iterate=False, continueAnywayHandler=None
    ):
        if self._workflow is None:
            self._workflow = WorkflowNodeModel(
                continueAction=continueAction,
                view=subview,
                nextModel=None,
                name=name,
                required=required,
                iterate=iterate,
                continueAnywayHandler=continueAnywayHandler,
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
                continueAnywayHandler=continueAnywayHandler,
            )
        return self

    def build(self):
        return Workflow(
            self._workflow,
            startLambda=self._startLambda,
            iterateLambda=self._iterateLambda,
            resetLambda=self._resetLambda,
            cancelLambda=self._cancelLambda,
            parent=self.parent,
        )
