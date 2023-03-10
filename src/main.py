from PyQt5 import QtGui, QtCore, QtWidgets
import sys

from snapred.ui.widget.MainWidget import DummyWidget

from mantidqt.widgets.instrumentview.api import get_instrumentview
from mantidqt.widgets.algorithmprogress import AlgorithmProgressWidget
from mantidqt.widgets.workspacewidget.workspacetreewidget import WorkspaceTreeWidget
from mantid.simpleapi import CreateSampleWorkspace

from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import ROOT_DIR

logger = snapredLogger.getLogger(__name__)

import snapred as sr
import inspect

class DummyGUI(QtWidgets.QMainWindow):

    def __init__(self, parent=None):
        super(DummyGUI, self).__init__(parent)

        dummyWidget = DummyWidget("load dummy", self)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        splitter.addWidget(dummyWidget.widget)
        
        # myiv = get_instrumentview(ws)
        # myiv.show_view()

        # import pdb; pdb.set_trace()
        splitter.addWidget(WorkspaceTreeWidget())
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
        asciiPath = ROOT_DIR + '/snapred/resources/ascii.txt'
        with open(asciiPath, 'r') as asciiArt:
            print(asciiArt.read())
        logger.info("Welcome User! Happy Reducing!")
        ex.show()
        app.exec_()
    except RuntimeError as error:
        ex = QtWidgets.QWidget()
        QtWidgets.QMessageBox.warning(ex, "Error", str(error))