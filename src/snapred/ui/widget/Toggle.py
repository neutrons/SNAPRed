from qtpy.QtCore import Property, QEasingCurve, QPropertyAnimation, Qt
from qtpy.QtGui import QLinearGradient, QPainter
from qtpy.QtWidgets import QWidget


class Toggle(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self._state = False
        self._ellipsePosition = 0.0
        self.toggleAnimation = QPropertyAnimation(self, b"ellipsePosition")
        # fixed height width ratio
        self.setFixedHeight(30)
        self.setFixedWidth(60)
        # self.update = self._doNothing
        self.toggleAnimation.finished.connect(self.update)

    @Property(float)
    def ellipsePosition(self):
        return self._ellipsePosition

    @ellipsePosition.setter
    def ellipsePosition(self, pos):
        self.update()
        self._ellipsePosition = pos

    def toggle(self):
        self.setState(not self._state)

    def setState(self, state):
        self._state = state
        self.animateClick()

    def connectUpdate(self, update):  # noqa: ARG002
        self.toggleAnimation.finished.connect(self.update)

    def _doNothing(self):
        pass

    def getState(self):
        return self._state

    def paintEvent(self, event):  # noqa: ARG002
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setPen(Qt.NoPen)
        painter.setBrush(Qt.gray if not self._state else Qt.darkGreen)
        # draw the background
        painter.drawRoundedRect(self.rect(), self.width(), self.height() / 2)
        # add gradient to background
        gradient = QLinearGradient(self.rect().topLeft(), self.rect().bottomLeft())
        gradient.setColorAt(1, Qt.darkCyan)
        gradient.setColorAt(0.3, Qt.gray if not self._state else Qt.green)
        painter.setBrush(gradient)
        painter.drawRoundedRect(self.rect(), self.width(), self.height() / 2)
        # draw the ellipse
        painter.setBrush(Qt.lightGray)
        painter.drawEllipse(int((self.width() / 2) * self._ellipsePosition), 0, int(self.width() / 2), self.height())

    def animateClick(self):
        # have ellipse slide to the opposite side
        self.toggleAnimation.setDuration(300)
        self.toggleAnimation.setEasingCurve(QEasingCurve.InOutQuart)
        if self._state:
            self.toggleAnimation.setStartValue(0)
            self.toggleAnimation.setEndValue(1.0)
        else:
            self.toggleAnimation.setStartValue(1.0)
            self.toggleAnimation.setEndValue(0)
        self.toggleAnimation.start()

    def mouseReleaseEvent(self, event):
        self.toggle()
        self.update()
        super().mouseReleaseEvent(event)
