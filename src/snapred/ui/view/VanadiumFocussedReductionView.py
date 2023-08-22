from PyQt5.QtWidgets import QLabel, QVBoxLayout, QWidget

from snapred.backend.dao.ingredients.VanadiumReductionIngredients import VanadiumReductionIngredients
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.meta.Config import Resource
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.VanadiumFocussedReductionPlot import VanadiumFoucussedReductionPlot
from snapred.ui.widget.WorkflowNode import finalizeWorkflow, startWorkflow


class VanadiumFocussedReductionView(BackendRequestView):
    def __init__(self, jsonForm, parent=None):
        selection = "vanadiumReduction/vanadiumReduction"
        super(VanadiumFocussedReductionView, self).__init__(jsonForm, selection, parent=parent)

        def vanadiumReductionFlow():
            ingredients = VanadiumReductionIngredients.parse_raw(
                Resource.read("default/request/vanadiumReduction/vanadiumReduction/payload.json")
            )
            request = SNAPRequest(path=selection, payload=ingredients.json())
            self.handleButtonClicked(request, self.beginFlowButton)

            def examineOutput():
                VanadiumFoucussedReductionPlot()
                workflow = startWorkflow(lambda workflow: None, self._labelView("Did it work?"))  # noqa: ARG005
                workflow = finalizeWorkflow(workflow, self)
                workflow.widget.show()

            self.worker.finished.connect(lambda: examineOutput())

        self.beginFlowButton.clicked.connect(vanadiumReductionFlow)

    def _labelView(self, text):
        win = QWidget()
        label = QLabel(text)
        vbox = QVBoxLayout()
        vbox.addWidget(label)
        vbox.addStretch()
        win.setLayout(vbox)
        return win
