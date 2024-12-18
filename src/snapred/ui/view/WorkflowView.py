from qtpy.QtCore import Slot
from qtpy.QtWidgets import QGridLayout, QTabWidget, QWidget

from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel
from snapred.ui.view.WorkflowNodeView import WorkflowNodeView


class WorkflowView(QWidget):
    def __init__(self, nodes: WorkflowNodeModel, parent=None):
        super(WorkflowView, self).__init__(parent)
        self.totalNodes = 0
        self.position = 0
        self.currentTab = 0

        # Do _not_ hide the `layout()` method!
        layout_ = QGridLayout()
        self.setLayout(layout_)

        # add a tab widget
        self.tabWidget = QTabWidget()
        self.tabWidget.setObjectName("nodeTabs")
        self.tabWidget.tabBarClicked.connect(self.handleTabClicked)
        layout_.addWidget(self.tabWidget)

        # add a tab for each node
        for node in nodes:
            self.tabWidget.addTab(self._generateTabWidget(node, self.totalNodes), node.name)
            self.totalNodes += 1
        # disable all tabs that are not the first
        for i in range(1, self.tabWidget.count()):
            self.tabWidget.setTabEnabled(i, False)

    @Slot(int)
    def handleTabClicked(self, index):
        # if clicked tab is enabled, set current tab to clicked tab
        if self.tabWidget.isTabEnabled(index):
            self.currentTab = index

    @Slot()
    def goBack(self):
        if self.currentTab > 0:
            self.currentTab -= 1
            self.tabWidget.setCurrentIndex(self.currentTab)

    @Slot()
    def goForward(self):
        if self.currentTab < self.position:
            self.currentTab += 1
            self.tabWidget.setCurrentIndex(self.currentTab)

    @property
    def tabModel(self):
        return self.tabWidget.widget(self.currentTab).model

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
        return self.tabWidget.widget(self.currentTab).continueButton

    @property
    def skipButton(self):
        return self.tabWidget.widget(self.currentTab).skipButton

    @property
    def cancelButton(self):
        return self.tabWidget.widget(self.currentTab).cancelButton

    def reset(self, hard: bool = False):
        """
        This resets the workflow
        """
        for i in range(0, self.tabWidget.count()):
            self.tabWidget.setTabEnabled(i, False)
            currentWidget = self.tabWidget.widget(i)
            currentWidget.continueButton.setEnabled(True)
            currentWidget.cancelButton.setEnabled(True)
            currentWidget.forwardButton.setVisible(False)
            if hard:
                currentWidget.reset()
        self.tabWidget.setTabEnabled(0, True)
        self.tabWidget.setCurrentIndex(0)
        self.currentTab = 0
        self.position = 0

    def advanceWorkflow(self):
        if self.currentTab < self.totalNodes - 1:
            # enable forward button of previous tab
            widget = self.tabWidget.widget(self.currentTab)
            widget.forwardButton.setVisible(True)
            # disable continue button of previous tab
            widget.continueButton.setEnabled(False)

            self.position = max(self.currentTab + 1, self.position)
            self.currentTab += 1
            self.tabWidget.setTabEnabled(self.currentTab, True)
            self.tabWidget.setCurrentIndex(self.currentTab)

    def _generateTabWidget(self, node, position):
        widget = WorkflowNodeView(node, position)
        widget.onBackButtonClicked(self.goBack)
        widget.onForwardButtonClicked(self.goForward)
        return widget
