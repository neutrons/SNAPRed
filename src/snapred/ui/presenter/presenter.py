from PyQt5 import QtGui, QtCore
from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.ReductionRequest import ReductionRequest
from snapred.backend.dao.RunConfig import RunConfig


from mantidqt.utils.asynchronous import AsyncTask


from PyQt5.QtCore import QObject, QThread, pyqtSignal
# Snip...

# Step 1: Create a worker class
class Worker(QObject):
    finished = pyqtSignal()
    result = pyqtSignal(object)
    progress = pyqtSignal(int)

    target = None
    args = None

    def __init__(self, target, args=None):
        super().__init__()
        self.target = target
        self.args = args

    def run(self):
        """Long-running task."""
        self.finished.emit()
        self.result.emit(self.target(self.args))




class LogTablePresenter(object):

    interfaceController = InterfaceController()

    def __init__(self, view, model):
        self.view = view
        self.model = model

        self.view.on_button_clicked(self.handle_button_clicked)

    @property
    def widget(self):
        return self.view
    
    def show(self):
      self.view.show()

    def update_reduction_config_element(self, reductionResponse):
        if reductionResponse.responseCode == 200:
            reductionConfig = reductionResponse.responseData  
            self.model.addRecipeConfig(reductionConfig)
            self.view.addRecipeConfig(self.model.getRecipeConfig())

    def handle_button_clicked(self):
        self.view.button.setEnabled(False)
        # Step 2: Create a QThread object
        self.thread = QThread()
        # Step 3: Create a worker object
        reductionRequest = ReductionRequest(mode="Reduction", runs=[RunConfig("000001")])
        self.worker = Worker(target=self.interfaceController.executeRequest, args=(reductionRequest))
        # Step 4: Move worker to the thread
        self.worker.moveToThread(self.thread)
        # Step 5: Connect signals and slots
        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.worker.result.connect(self.update_reduction_config_element)
        # Step 6: Start the thread
        self.thread.start()

        # Final resets
        self.thread.finished.connect(
            lambda: self.view.button.setEnabled(True)
        )