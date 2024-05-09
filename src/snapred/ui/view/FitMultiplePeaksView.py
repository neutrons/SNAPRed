from qtpy.QtWidgets import QLabel, QVBoxLayout, QWidget

from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.data.GroceryService import GroceryService
from snapred.meta.Config import Resource
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.FitPeaksPlot import FitPeaksPlot
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder


class FitMultiplePeaksView(BackendRequestView):
    def __init__(self, jsonForm, parent=None):
        selection = "fitMultiplePeaks/fitMultiplePeaks"
        super(FitMultiplePeaksView, self).__init__(jsonForm, selection, parent=parent)

        def fitPeaksFlow():
            GroceryService().fetchWorskpace(
                path="/SNS/SNAP/shared/Malcolm/Temp/DSP_57514_calibFoc.nxs",
                name="testWS",
                loader="LoadNexusProcessed",
            )
            # TODO: Once jsonForm can correctly parse the input this will not be required
            ingredients = Ingredients.parse_raw(
                Resource.read("default/request/fitMultiplePeaks/fitMultiplePeaks/payload.json")
            )
            request = SNAPRequest(path="fitMultiplePeaks/fitMultiplePeaks", payload=ingredients.json())
            self.handleButtonClicked(request, self.beginFlowButton)

            def examineOutput():
                FitPeaksPlot("testWS")
                workflow = (
                    WorkflowBuilder(parent)
                    .addNode(lambda workflow: None, self._labelView("Did it work?"))  # noqa: ARG005, E501
                    .build()
                )
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
