# a pyqt progress bar view
from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import QProgressBar


class ProgressView(QProgressBar):
    updateProgressSignal = Signal(int)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimum(0)
        self.setMaximum(100)
        self.setValue(0)
        self.setFormat("Progress: %p%")
        self.setTextVisible(True)
        self.updateProgressSignal.connect(self._updateProgress)

    def updateProgress(self, value: int):
        self.updateProgressSignal.emit(value)

    @Slot(int)
    def _updateProgress(self, value):
        self.setValue(value)
