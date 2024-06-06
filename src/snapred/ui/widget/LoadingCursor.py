from qtpy.QtCore import Qt
from qtpy.QtWidgets import QApplication, QWidget


class LoadingCursor(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(Qt.CustomizeWindowHint | Qt.FramelessWindowHint)
        self.setWindowModality(Qt.ApplicationModal)
        self.setStyleSheet("background-color: rgba(0, 0, 0, 50%);")

    def showEvent(self, event):
        QApplication.setOverrideCursor(Qt.WaitCursor)
        super().showEvent(event)

    def closeEvent(self, event):
        QApplication.restoreOverrideCursor()
        super().closeEvent(event)
