from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.presenter.WorkflowPresenter import WorkflowPresenter


class Workflow:
    def __init__(self, model: WorkflowNodeModel, cancelLambda=None, iterateLambda=None, parent=None):
        # default loading subview
        self._presenter = WorkflowPresenter(
            model, cancelLambda=cancelLambda, iterateLambda=iterateLambda, parent=parent
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
