import sys

from qtpy.QtCore import Property, QEasingCurve, QPropertyAnimation, Qt, Signal, Slot
from qtpy.QtGui import QColor, QLinearGradient, QPainter
from qtpy.QtWidgets import QSizePolicy, QWidget

## "sphinx" work-around:
if "sphinx" in sys.modules:
    # Why do we use sphinx?
    # "sphinx" can't deal with `Slot(QColor)` or `Property(QColor)`,
    #   but Python itself can.
    QColor = object

# HOW TO USE QSS to modify the appearance of this widget (now set using `src/snapred/resources/style.qss`):
_styleSheet = """
    /* DEFAULT values for all `Toggle`: */

    .Toggle {
        /* fixed height and width */
        min-height: 30px; max-height: 30px;
        min-width: 60px; max-width: 60px;

        /* Toggle-switch background */
        qproperty-backgroundColor: rgb(0, 128, 0); /* Qt.darkGreen */

        /* Toggle-switch bar: vertical gradient */
        qproperty-gradStartColor: rgb(0, 255, 0);
        qproperty-gradEndColor: rgb(0, 128, 128); /* Qt.darkCyan */
    }

    /* OVERRIDE for a _specific_ _named_ `Toggle`:
         use `self.field.setObjectName("the_object_name")`. */

    /* WARNING: this is the name of the `LabeledField.field`, which is of class `Toggle`,
         not the `LabeledField` itself! */

    Toggle#the_object_name {
        /* Whatever you want to override goes in here: */
    }
"""


class Toggle(QWidget):
    stateChanged = Signal(bool)

    def __init__(self, parent=None, state=False):
        super().__init__(parent=parent)

        # Setting this size policy here allows a parent `QWidget` (e.g. `LabeledField`)
        #   to automatically set its `sizePolicy`.
        #   => only allow expansion in the horizontal direction.
        self.setSizePolicy(QSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed))

        self._state = state
        self._ellipsePosition = 1.0 if state else 0.0
        self.toggleAnimation = QPropertyAnimation(self, b"ellipsePosition")

        # Complete the animation _first_, and only _then_ emit the stateChange signal:
        self.toggleAnimation.finished.connect(lambda: self.stateChanged.emit(self._state))
        self.toggleAnimation.finished.connect(self.update)

        # The following properties are now set using the SNAPRed application's style sheet:
        #   fixed height width
        # self.setFixedHeight(30)
        # self.setFixedWidth(60)
        self._backgroundColor = QColor(0, 128, 0)  # will be overridden by the style sheet
        self._gradStartColor = QColor(0, 255, 0)  # " "
        self._gradEndColor = QColor(0, 128, 128)  # " "

        # Do _not_ animate at initial setup!
        # self.animateClick()

    @Property(float)
    def ellipsePosition(self):
        return self._ellipsePosition

    @ellipsePosition.setter
    def ellipsePosition(self, pos):
        self.update()
        self._ellipsePosition = pos

    def toggle(self):
        self.setState(not self._state)

    @Slot(bool)
    def setState(self, state):
        if self._state != state:
            self._state = state
            if self.isEnabled():
                # animate first, _then_ emit the signal
                self.animateClick()
            else:
                # IMPORTANT: do not animate or emit `stateChange` when disabled!
                self._ellipsePosition = 1.0 if state else 0.0

    def connectUpdate(self, update):  # noqa: ARG002
        # TODO: Is this method actually used anywhere?
        self.toggleAnimation.finished.connect(update)

    def getState(self):
        return self._state

    @Property(QColor)
    def backgroundColor(self):
        # Be careful here: don't hide the widget's `background-color` property (or function)!
        return self._backgroundColor

    @backgroundColor.setter
    def backgroundColor(self, color: QColor):
        self._backgroundColor = color

    @Property(QColor)
    def gradStartColor(self):
        return self._gradStartColor

    @gradStartColor.setter
    def gradStartColor(self, color: QColor):
        self._gradStartColor = color

    @Property(QColor)
    def gradEndColor(self):
        return self._gradEndColor

    @gradEndColor.setter
    def gradEndColor(self, color: QColor):
        self._gradEndColor = color

    def paintEvent(self, event):  # noqa: ARG002
        backgroundColor = self.backgroundColor
        gradStartColor = self.gradStartColor
        gradEndColor = self.gradEndColor

        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setPen(Qt.NoPen)

        # draw the background
        painter.setBrush(Qt.gray if not self._state else backgroundColor)
        painter.drawRoundedRect(self.rect(), self.width(), self.height() / 2)

        # add gradient to background
        gradient = QLinearGradient(self.rect().topLeft(), self.rect().bottomLeft())
        gradient.setColorAt(1.0, gradEndColor)
        gradient.setColorAt(0.3, Qt.gray if not self._state else gradStartColor)
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
        super().mouseReleaseEvent(event)
