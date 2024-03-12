import math

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QHBoxLayout, QLineEdit, QMessageBox, QSlider, QWidget


class SmoothingSlider(QWidget):
    """
    A custom QWidget in PyQt5 tailored for adjusting a smoothing parameter via both a graphical
    slider and a precise numerical input field. This design marries the intuitive adjustability
    of a slider with the exactitude of direct number entry, accommodating diverse user preferences
    for either rapid exploration of smoothing levels or specific value input.

    Key Features:

    - Dual Interface: Incorporates a QSlider for graphical adjustments alongside a QLineEdit for
      precise value entry.
    - Custom Value Mapping: Logarithmically maps the slider's range (-1000 to 0) to a broad and
      finely controllable range of smoothing parameter values.
    - Styling: Enhances user interaction through customized CSS styling for the slider.
    - Validation and Error Handling: Ensures validity of entered numerical values, prompting users
      with warnings for incorrect inputs.

    Functionalities:

    - Value Conversion: Employs a logarithmic conversion mechanism for intuitive and resolution-friendly
      parameter adjustment.
    - Synchronization Between Controls: Maintains consistency between slider and text field,
      updating each based on changes to the other to prevent user confusion and errors.
    - Error Handling: Implements input validation to restrict entries to non-negative numbers,
      displaying warnings for invalid inputs to safeguard against erroneous data manipulations.
    - Programmatic Value Setting: Facilitates dynamic value updates through a setValue method,
      enabling easy integration into larger workflows or automated setups.

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
