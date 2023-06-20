"""
    Elypson/qt-collapsible-section
    (c) 2016 Michael A. Voelkel - michael.alexander.voelkel@gmail.com

    This file is part of Elypson/qt-collapsible section.

    Elypson/qt-collapsible-section is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, version 3 of the License, or
    (at your option) any later version.

    Elypson/qt-collapsible-section is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Elypson/qt-collapsible-section. If not, see <http:#www.gnu.org/licenses/>.

    Modified by Michael Walsh (walshmm@ornl.gov)
"""

# import PyQt5.QtGui as gui

import PyQt5.QtCore as cr
import PyQt5.QtWidgets as wd


class Section(wd.QWidget):
    def __init__(self, title="", animationDuration=100, parent=None):
        super().__init__(parent)
        self.parentSections = []
        self.childSections = []
        self.collapsedHeight = 0
        self.contentHeight = 0
        self.setStyleSheet("background-color: #F5E9E2;")
        self.animationDuration = animationDuration
        self.toggleButton = wd.QToolButton(self)
        self.headerLine = wd.QFrame(self)
        self.toggleAnimation = cr.QParallelAnimationGroup(self)
        self.contentArea = wd.QScrollArea(self)
        self.mainLayout = wd.QGridLayout(self)
        self.mainLayout.columnStretch(1)
        self.mainLayout.rowStretch(1)

        self.toggleButton.setStyleSheet("QToolButton {border: none;}")
        self.toggleButton.setToolButtonStyle(cr.Qt.ToolButtonTextBesideIcon)
        self.toggleButton.setArrowType(cr.Qt.RightArrow)
        self.toggleButton.setText(title)
        self.toggleButton.setCheckable(True)
        self.toggleButton.setChecked(False)

        self.headerLine.setFrameShape(wd.QFrame.HLine)
        self.headerLine.setFrameShadow(wd.QFrame.Sunken)
        self.headerLine.setSizePolicy(wd.QSizePolicy.Expanding, wd.QSizePolicy.Maximum)

        # self.contentArea.setLayout(wd.QHBoxLayout())
        self.contentArea.setSizePolicy(wd.QSizePolicy.Expanding, wd.QSizePolicy.Maximum)

        # start out collapsed
        self.contentArea.setMaximumHeight(0)
        self.contentArea.setMinimumHeight(0)
        self.contentArea.setLayout(wd.QVBoxLayout())

        self.minHeightAnimation = cr.QPropertyAnimation(self, b"minimumHeight")
        self.maxHeightAnimation = cr.QPropertyAnimation(self, b"maximumHeight")
        self.maxHeightContentAnimation = cr.QPropertyAnimation(self.contentArea, b"maximumHeight")
        # let the entire widget grow and shrink with its content
        self.toggleAnimation.addAnimation(self.minHeightAnimation)
        self.toggleAnimation.addAnimation(self.maxHeightAnimation)
        self.toggleAnimation.addAnimation(self.maxHeightContentAnimation)

        self.mainLayout.setVerticalSpacing(0)
        self.mainLayout.setContentsMargins(0, 0, 0, 0)

        row = 0
        self.mainLayout.addWidget(self.toggleButton, row, 0, 1, 1, cr.Qt.AlignLeft)
        self.mainLayout.addWidget(self.headerLine, row, 2, 1, 1)
        self.mainLayout.addWidget(self.contentArea, row + 1, 0, 1, 3)
        self.setLayout(self.mainLayout)

        self.toggleButton.toggled.connect(self.toggle)

    def appendWidget(self, widget):
        layout = self.contentArea.layout()
        if layout is None:
            layout = wd.QVBoxLayout()
        layout.addWidget(widget)
        self.contentArea.setLayout(layout)
        if type(widget) == Section:
            widget.parentSections.append(self)
            self.childSections.append(widget)
        self.adjustAnimations()

    def adjustAnimations(self):
        contentLayout = self.contentArea.layout()
        self.collapsedHeight = self.sizeHint().height() - self.contentArea.maximumHeight()
        self.contentHeight = contentLayout.sizeHint().height()
        self.updateAnimationHeight(self.contentHeight)

    def updateAnimationHeight(self, contentHeight):
        for SectionAnimation in [self.minHeightAnimation, self.maxHeightAnimation]:
            SectionAnimation.setDuration(self.animationDuration)
            SectionAnimation.setStartValue(self.collapsedHeight)
            SectionAnimation.setEndValue(self.collapsedHeight + contentHeight)
        contentAnimation = self.maxHeightContentAnimation
        contentAnimation.setDuration(self.animationDuration)
        contentAnimation.setStartValue(0)
        contentAnimation.setEndValue(contentHeight)

    def adjustParentAnimationHeight(self, height):
        for parent in self.parentSections:
            parent.addHeight(height)
            parent.adjustParentAnimationHeight(height)

    def addHeight(self, height):
        self.contentHeight = self.contentHeight + height
        self.updateAnimationHeight(self.contentHeight)
        self.toggleAnimation.start()

    def toggle(self, collapsed):
        height = self.contentArea.layout().sizeHint().height() + self.collapsedHeight * 2
        if collapsed:
            self.adjustParentAnimationHeight(height)
            self.toggleButton.setArrowType(cr.Qt.DownArrow)
            self.toggleAnimation.setDirection(cr.QAbstractAnimation.Forward)
        else:
            self.adjustParentAnimationHeight(-height)
            self.toggleButton.setArrowType(cr.Qt.RightArrow)
            self.toggleAnimation.setDirection(cr.QAbstractAnimation.Backward)

        self.toggleAnimation.start()
