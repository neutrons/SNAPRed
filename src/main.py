from PyQt5 import QtGui, QtCore, QtWidgets
import sys

from snapred.ui.widget.MainWidget import DummyWidget

from mantidqt.widgets.instrumentview.api import get_instrumentview
from mantidqt.widgets.algorithmprogress import AlgorithmProgressWidget
from mantid.simpleapi import CreateSampleWorkspace

import snapred as sr
import inspect

class DummyGUI(QtWidgets.QMainWindow):

    def __init__(self, parent=None):
        super(DummyGUI, self).__init__(parent)

        dummyWidget = DummyWidget("load dummy", self)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        splitter.addWidget(dummyWidget.widget)
        ws = CreateSampleWorkspace()
        # myiv = get_instrumentview(ws)
        # myiv.show_view()

        # import pdb; pdb.set_trace()

        splitter.addWidget(AlgorithmProgressWidget())

        self.setCentralWidget(splitter)
        self.setWindowTitle("DummyGUI")


def qapp():
    if QtWidgets.QApplication.instance():
        _app = QtWidgets.QApplication.instance()
    else:
        _app = QtWidgets.QApplication(sys.argv)
    return _app


if __name__ == "__main__":
    app = qapp()
    try:
        ex = DummyGUI()
        #ex.resize(700, 700)
        ex.show()
        app.exec_()
    except RuntimeError as error:
        ex = QtWidgets.QWidget()
        QtWidgets.QMessageBox.warning(ex, "Error", str(error))