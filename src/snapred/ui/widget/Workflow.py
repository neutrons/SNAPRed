from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.presenter.WorkflowPresenter import WorkflowPresenter


class Workflow:
    def __init__(
        self,
        model: WorkflowNodeModel,
        *,
        startLambda=None,
        iterateLambda=None,
        resetLambda=None,
        cancelLambda=None,
        parent=None,
    ):
        # default loading subview
        self._presenter = WorkflowPresenter(
            model,
            startLambda=startLambda,
            iterateLambda=iterateLambda,
            resetLambda=resetLambda,
            cancelLambda=cancelLambda,
            parent=parent,
        )

    @property
    def presenter(self):
        return self._presenter

    @property
    def widget(self):
        return self._presenter.widget

    @property
    def nextWidget(self):
        return self._presenter.nextView

    def show(self):
        self._presenter.show()
