from PyQt5.QtWidgets import QGridLayout, QMainWindow, QPushButton, QTabWidget, QWidget

from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.view.WorkflowNodeView import WorkflowNodeView


class WorkflowView(QWidget):
    def __init__(self, nodes: WorkflowNodeModel, parent=None):
        super(WorkflowView, self).__init__(parent)
        self.totalNodes = 0
        self.position = 0
        self.currentTab = 0

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        # add a tab widget
        self.tabWidget = QTabWidget()
        self.tabWidget.tabBarClicked.connect(self.handleTabClicked)
        self.layout.addWidget(self.tabWidget)

        # add a tab for each node
        for node in nodes:
            self.tabWidget.addTab(self._generateTabWidget(node, self.totalNodes), node.name)
            self.totalNodes += 1
        # disable all tabs that are not the first
        for i in range(1, self.tabWidget.count()):
            self.tabWidget.setTabEnabled(i, False)

    def handleTabClicked(self, index):
        # if clicked tab is enabled, set current tab to clicked tab
        if self.tabWidget.isTabEnabled(index):
            self.currentTab = index

    def goBack(self):
        if self.currentTab > 0:
            self.currentTab -= 1
            self.tabWidget.setCurrentIndex(self.currentTab)

    def goForward(self):
        if self.currentTab < self.position:
            self.currentTab += 1
            self.tabWidget.setCurrentIndex(self.currentTab)

    @property
    def tabView(self):
        return self.tabWidget.widget(self.currentTab).view

    @property
    def nextTabView(self):
        if self.currentTab + 1 < self.totalNodes - 1:
            return self.tabWidget.widget(self.currentTab + 1).view
        else:
            return None

    @property
    def continueButton(self):
        widget = self.tabWidget.widget(self.currentTab)
        return widget.continueButton

    @property
    def cancelButton(self):
        widget = self.tabWidget.widget(self.currentTab)
        return widget.cancelButton

    def advanceWorkflow(self):
        if self.currentTab < self.totalNodes - 1:
            # enable forward button of previous tab
            widget = self.tabWidget.widget(self.currentTab)
            widget.forwardButton.setVisible(True)
            # disable continue button of previous tab
            widget.continueButton.setEnabled(False)
            # TODO: Forward fields from previous tab to next tab
            self.position = max(self.currentTab + 1, self.position)
            self.currentTab += 1
            self.tabWidget.setTabEnabled(self.currentTab, True)
            self.tabWidget.setCurrentIndex(self.currentTab)

    def _generateTabWidget(self, node, position):
        widget = WorkflowNodeView(node, position)
        widget.onBackButtonClicked(self.goBack)
        widget.onForwardButtonClicked(self.goForward)
        return widget
