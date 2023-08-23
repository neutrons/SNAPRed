import os

from mantid.simpleapi import LoadNexusProcessed
from PyQt5.QtWidgets import QLabel, QVBoxLayout, QWidget

from snapred.backend.dao.ingredients import FitMultiplePeaksIngredients
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.meta.Config import Resource
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.FitPeaksPlot import FitPeaksPlot
from snapred.ui.widget.WorkflowNode import finalizeWorkflow, startWorkflow


class FitMultiplePeaksView(BackendRequestView):
    def __init__(self, jsonForm, parent=None):
        selection = "fitMultiplePeaks/fitMultiplePeaks"
        super(FitMultiplePeaksView, self).__init__(jsonForm, selection, parent=parent)

        def fitPeaksFlow():
            LoadNexusProcessed(
                Filename="/SNS/SNAP/shared/Malcolm/Temp/DSP_57514_calibFoc.nxs", OutputWorkspace="testWS"
            )
            # TODO: Once jsonForm can correctly parse the input this will not be required
            ingredients = FitMultiplePeaksIngredients.parse_raw(
                Resource.read("default/request/fitMultiplePeaks/fitMultiplePeaks/payload.json")
            )
            request = SNAPRequest(path="fitMultiplePeaks/fitMultiplePeaks", payload=ingredients.json())
            self.handleButtonClicked(request, self.beginFlowButton)

            def examineOutput():
                FitPeaksPlot("testWS")
                workflow = startWorkflow(lambda workflow: None, self._labelView("Did it work?"))  # noqa: ARG005
                workflow = finalizeWorkflow(workflow, self)
                workflow.widget.show()

            self.worker.finished.connect(lambda: examineOutput())

        self.beginFlowButton.clicked.connect(fitPeaksFlow)

    def _labelView(self, text):
        win = QWidget()
        label = QLabel(text)
        vbox = QVBoxLayout()
        vbox.addWidget(label)
        vbox.addStretch()
        win.setLayout(vbox)
        return win
