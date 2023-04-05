from PyQt5 import QtGui, QtCore, QtWidgets
from PyQt5.QtGui import QPalette
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
import sys

from snapred.ui.widget.MainWidget import DummyWidget

from mantidqt.widgets.instrumentview.api import get_instrumentview
from mantidqt.widgets.algorithmprogress import AlgorithmProgressWidget
from mantidqt.widgets.workspacewidget.workspacetreewidget import WorkspaceTreeWidget
from mantid.simpleapi import CreateSampleWorkspace


from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Resource

logger = snapredLogger.getLogger(__name__)

import snapred as sr
import inspect


class MyBar(QWidget):
    clickPos = None
    def __init__(self, parent):
        super(MyBar, self).__init__(parent)
        self.setAutoFillBackground(True)
        
        self.setBackgroundRole(QPalette.Shadow)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(1, 1, 1, 1)
        layout.addStretch()

        self.title = QLabel("My Own Bar", self, alignment=Qt.AlignCenter)
        self.title.setObjectName("headerTitle")
        
        self.setObjectName("header")
        # if setPalette() was used above, this is not required
        self.title.setForegroundRole(QPalette.Light)

        style = self.style()
        ref_size = self.fontMetrics().height()
        ref_size += style.pixelMetric(style.PM_ButtonMargin) * 2
        self.setMaximumHeight(ref_size + 2)

        btn_size = QSize(ref_size, ref_size)
        for target in ('min', 'normal', 'max', 'close'):
            btn = QToolButton(self, focusPolicy=Qt.NoFocus)
            layout.addWidget(btn)
            btn.setFixedSize(btn_size)

            iconType = getattr(style, 
                'SP_TitleBar{}Button'.format(target.capitalize()))
            btn.setIcon(style.standardIcon(iconType))

            if target == 'close':
                colorNormal = 'red'
                colorHover = 'orangered'
            else:
                colorNormal = 'palette(mid)'
                colorHover = 'palette(light)'
            btn.setStyleSheet('''
                QToolButton {{
                    background-color: {};
                }}
                QToolButton:hover {{
                    background-color: {}
                }}
            '''.format(colorNormal, colorHover))

            signal = getattr(self, target + 'Clicked')
            btn.clicked.connect(signal)

            setattr(self, target + 'Button', btn)

        self.normalButton.hide()

        self.updateTitle(parent.windowTitle())
        parent.windowTitleChanged.connect(self.updateTitle)

    def updateTitle(self, title=None):
        if title is None:
            title = self.window().windowTitle()
        width = self.title.width()
        width -= self.style().pixelMetric(QStyle.PM_LayoutHorizontalSpacing) * 3
        self.title.setText(self.fontMetrics().elidedText(
            title, Qt.ElideRight, width))

    def windowStateChanged(self, state):
        self.normalButton.setVisible(state == Qt.WindowMaximized)
        self.maxButton.setVisible(state != Qt.WindowMaximized)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.clickPos = event.windowPos().toPoint()

    def mouseMoveEvent(self, event):
        if self.clickPos is not None:
            self.window().move(event.globalPos() - self.clickPos)

    def mouseReleaseEvent(self, QMouseEvent):
        self.clickPos = None

    def closeClicked(self):
        self.window().close()

    def maxClicked(self):
        self.window().showMaximized()

    def normalClicked(self):
        self.window().showNormal()

    def minClicked(self):
        self.window().showMinimized()

    def resizeEvent(self, event):
        self.title.resize(self.width(), self.height())
        self.updateTitle()

class SNAPRedGUI(QtWidgets.QMainWindow):

    def __init__(self, parent=None):
        super(SNAPRedGUI, self).__init__(parent)
        self.setAttribute(Qt.WA_TranslucentBackground, True)
        self.setAttribute(Qt.WA_DontCreateNativeAncestors, True)
        dummyWidget = DummyWidget("load dummy", self)
        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        splitter.addWidget(dummyWidget.widget)
        
        # myiv = get_instrumentview(ws)
        # myiv.show_view()

        # import pdb; pdb.set_trace()
        workspaceTreeWidgetWrapper = QWidget()
        workspaceTreeWidgetWrapperLayout = QHBoxLayout()
        workspaceTreeWidget = WorkspaceTreeWidget()
        workspaceTreeWidgetWrapper.setObjectName("workspaceTreeWidget")
        workspaceTreeWidgetWrapperLayout.addWidget(workspaceTreeWidget, alignment=Qt.AlignCenter)
        workspaceTreeWidgetWrapper.setLayout(workspaceTreeWidgetWrapperLayout)
        splitter.addWidget(workspaceTreeWidgetWrapper)
        splitter.addWidget(AlgorithmProgressWidget())
        
        centralWidget = QWidget()
        centralWidget.setObjectName("centralwidget")
        centralLayout = QVBoxLayout()
        centralWidget.setLayout(centralLayout)
        
        self.setCentralWidget(centralWidget)
        self.setWindowTitle("SNAPRed")
        self.setWindowFlags(self.windowFlags() | Qt.FramelessWindowHint)

        self.titleBar = MyBar(centralWidget)
        centralLayout.addWidget(self.titleBar)
        centralLayout.addWidget(splitter)

        self.setContentsMargins(0, self.titleBar.height(), 0, 0)

        self.resize(320, self.titleBar.height() + 480)
        self.setMinimumSize(320, self.titleBar.height() + 480)
        
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        
        
    def closeEvent(self, event):
        event.accept()
        
    def changeEvent(self, event):
        if event.type() == event.WindowStateChange:
            self.titleBar.windowStateChanged(self.windowState())

    def resizeEvent(self, event):
        self.titleBar.resizeEvent(event)

def qapp():
    if QtWidgets.QApplication.instance():
        _app = QtWidgets.QApplication.instance()
    else:
        _app = QtWidgets.QApplication(sys.argv)
    return _app


if __name__ == "__main__":
    app = qapp()
    with Resource.open('style.qss', 'r') as styleSheet:
            app.setStyleSheet(styleSheet.read())
    try:
        ex = SNAPRedGUI()
        
        #ex.resize(700, 700)
        asciiPath = 'ascii.txt'
        with Resource.open(asciiPath, 'r') as asciiArt:
            print(asciiArt.read())
        logger.info("Welcome User! Happy Reducing!")
        ex.show()
        ret = app.exec_()
        sys.exit(ret)
        
    except Exception as uncaughtError:
        ex = QtWidgets.QWidget()
        QtWidgets.QMessageBox.critical(ex, "Uncaught Error!", str(uncaughtError))