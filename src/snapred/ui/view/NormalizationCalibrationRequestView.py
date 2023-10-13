from PyQt.QtCore import pyqtSignal
from PyQt5.QtWidgets import QGridLayout, QLabel, QLineEdit, QWidget

from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField

class NormalizationCalibrationRequestView(BackendRequestView):
    def __init__(self, jsonForm, calibrantsamples=[], parent=None):
