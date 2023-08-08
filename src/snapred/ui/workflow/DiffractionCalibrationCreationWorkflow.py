from PyQt5.QtWidgets import QLabel

from snapred.ui.view.CalibrationReductionRequestView import CalibrationReductionRequestView
from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder


class DiffractionCalibrationCreationWorkflow:
    def __init__(self, jsonForm, parent=None):
        # create a tree of flows for the user to successfully execute diffraction calibration
        # Calibrate     ->
        # Assess        ->
        # Save?         ->
        self.workflow = (
            WorkflowBuilder(parent)
            .addNode(
                DiffractionCalibrationCreationWorkflow._triggerCalibrationReduction,
                CalibrationReductionRequestView(jsonForm, parent=parent),
                "Calibrating",
            )
            .addNode(
                DiffractionCalibrationCreationWorkflow._assessCalibration, QLabel("Assessing Calibration"), "Assessing"
            )
            .addNode(DiffractionCalibrationCreationWorkflow._saveCalibration, QLabel("Saving Calibration"), "Saving")
            .build()
        )

    def _triggerCalibrationReduction(workflowPresenter):
        pass

    def _assessCalibration(workflowPresenter):
        # Load Previous ->
        pass

    def _saveCalibration(workflowPresenter):
        pass

    @property
    def widget(self):
        return self.workflow.presenter.widget

    def show(self):
        # wrap workflow.presenter.widget in a QMainWindow
        # show the QMainWindow
        pass
