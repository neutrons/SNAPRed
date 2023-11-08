from qtpy.QtWidgets import QComboBox

from snapred.ui.view.BackendRequestView import BackendRequestView


class NormalizationCalibrationRequestView(BackendRequestView):
    def __init__(self, jsonForm, samplePaths=[], groups=[], parent=None):
        selection = "calibration/reduction"
        super(NormalizationCalibrationRequestView, self).__init__(jsonForm, selection, parent=parent)
        self.runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        self.emptyRunNumberField = self._labeledField("Empty Run Number", jsonForm.getField("emptyRunNumber"))
        self.smoothingParameter = self._labeledField("Smoothing Parameter", jsonForm.getField("smoothingParameter"))

        """ inputs for absorption correction:
                inputWS - user only inputs a run number so, the naming schema will be based on that value
                backgroundWS
                CalibrationWS
                ReductionIngredients
                CalibrantSample
                OutputWS - same as inputWS
        """

        self.sampleDropDown = self._sampleDropDown("Sample", samplePaths)
        self.groupingFileDropDown = self._sampleDropDown("Grouping File", groups)

        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.emptyRunNumberField, 0, 1)
        self.layout.addWidget(self.smoothingParameter, 1, 0)
        self.layout.addWidget(self.sampleDropDown, 2, 0)
        self.layout.addWidget(self.groupingFileDropDown, 2, 1)

    def verify(self):
        if self.sampleDropDown.currentIndex() == 0:
            raise ValueError("Please select a sample")
        if self.groupingFileDropDown.currentIndex() == 0:
            raise ValueError("Please select a grouping file")
        return True
