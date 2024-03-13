import math

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QHBoxLayout, QLineEdit, QMessageBox, QSlider, QWidget


class SmoothingSlider(QWidget):
    """

    This PyQt5 custom QWidget is designed to facilitate the adjustment of a smoothing parameter, combining a graphical
    slider for intuitive control with a numerical input for precision. It caters to user preferences for either a quick
    visual adjustment or specific numerical entry, enhancing the usability of applications requiring fine-tuned
    parameter settings. The widget features logarithmic mapping for the slider to cover a broad range of values and
    includes synchronization between controls to ensure consistency and accuracy.

    """

    def __init__(self, parent=None, state=1.0):
        super().__init__(parent)
        self._slider = QSlider(Qt.Horizontal)
        self._slider.setMinimum(-1000)
        self._slider.setMaximum(0)
        self._slider.setValue(0)
        self._slider.setTickInterval(1)
        self._slider.setSingleStep(1)
        self._slider.setStyleSheet(
            "QSlider::groove:horizontal {"
            "border: 1px solid #999999;"
            "height: 8px;"
            "background: red;"
            "margin: 2px 0;"
            "}"
            "QSlider::handle:horizontal {"
            "background: white;"
            "border: 1px solid #5c5c5c;"
            "width: 18px;"
            "margin: -2px 0;"
            "border-radius: 3px;"
            "}"
        )

        self._number = QLineEdit(str(state))
        self._number.setMinimumWidth(128)

        self.layout = QHBoxLayout()
        self.setLayout(self.layout)
        self.layout.addWidget(self._slider)
        self.layout.addWidget(self._number)

        self._slider.valueChanged.connect(self._updateNumberFromSlider)
        self._number.editingFinished.connect(self._updateSliderFromNumber)

    def _updateNumberFromSlider(self):
        v = self._slider.value() / 100.0
        s = 10**v
        self._number.setText("{:.2e}".format(s))

    def _updateSliderFromNumber(self):
        text = self._number.text()
        try:
            s = float(text)
            assert s >= 0.0
        except ValueError:
            self._updateNumberFromSlider()
            QMessageBox.warning(
                self,
                "Non-numerical value in slider",
                "Smoothing parameter must be a numerical value.",
            )
            return
        except AssertionError:
            self._updateNumberFromSlider()
            QMessageBox.warning(
                self,
                "Negative number in slider",
                "Smoothing parameter must be a nonnegative number.",
            )
            return
        s = float(text)
        v = math.log10(s)
        sliderValue = int(v * 100)
        self._slider.setValue(sliderValue)

    def setValue(self, v):
        self._number.setText(str(v))
        self._updateSliderFromNumber()

    def value(self):
        return float(self._number.text())

    def connectUpdate(self, update):  # noqa: ARG002
        self.toggleAnimation.finished.connect(update)
