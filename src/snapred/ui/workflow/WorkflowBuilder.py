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
        self._invisibleNodes = []

    def addNode(
        self,
        continueAction,
        subview,
        name="Unnamed",
        required=True,
        iterate=False,
        continueAnywayHandler=None,
        visible=True,
    ):
        """
        Adds a node to the workflow. If visible=False, the node is initially hidden.
        """
        node = WorkflowNodeModel(
            continueAction=continueAction,
            view=subview,
            nextModel=None,
            name=name,
            required=required,
            iterate=iterate,
            continueAnywayHandler=continueAnywayHandler,
        )

        # If the node is invisible, add it to the invisible nodes list
        if not visible:
            self._invisibleNodes.append(node)

        if self._workflow is None:
            self._workflow = node
        else:
            currentWorkflow = self._workflow
            while currentWorkflow.nextModel is not None:
                currentWorkflow = currentWorkflow.nextModel
            currentWorkflow.nextModel = node

        return self

    def makeNodeVisible(self, nodeName):
        """
        Makes an invisible node visible by name.
        """
        for node in self._invisibleNodes:
            if node.name == nodeName:
                node.view.setVisible(True)  # Set the node's view to be visible
                self._invisibleNodes.remove(node)  # Remove from the invisible list
                break

    def build(self):
        return Workflow(
            self._workflow,
            startLambda=self._startLambda,
            iterateLambda=self._iterateLambda,
            resetLambda=self._resetLambda,
            cancelLambda=self._cancelLambda,
            parent=self.parent,
        )
