from qtpy.QtCore import QObject

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.ui.view.ProgressView import ProgressView


class ProgressPresenter(QObject):
    def __init__(self, view: ProgressView):
        super().__init__()

        # `InterfaceController` and `WorkerPool` are singletons:
        #   declaring them as instance attributes, rather than class attributes,
        #   allows singleton reset during testing.
        self.interfaceController = InterfaceController()
        self.view = view
        self.interfaceController.subscribe("progress", self.updateProgress)

    def updateProgress(self, progressData):
        if "dt_est" in progressData and progressData["dt_est"] is not None and progressData["dt_est"] > 0:
            percent = int(100 * (1 - progressData["dt_rem"] / progressData["dt_est"]))
            self.view.updateProgress(percent)
        else:
            self.view.updateProgress(0)
