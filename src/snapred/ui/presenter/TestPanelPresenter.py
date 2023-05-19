import os

from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material
from snapred.meta.Config import Resource
from snapred.ui.threading.worker_pool import WorkerPool


class TestPanelPresenter(object):
    interfaceController = InterfaceController()
    worker_pool = WorkerPool()

    def __init__(self, view):
        self.view = view

        self.view.calibrationReductinButtonOnClick(self.handleCalibrationReductinButtonClicked)
        self.view.calibrationIndexButtonOnClick(self.handleCalibrationIndexButtonClicked)
        self.view.calibrantSampleButtonOnClick(self.handleCalibrantSampleButtonClicked)

    @property
    def widget(self):
        return self.view

    def show(self):
        self.view.show()

    def handleCalibrationReductinButtonClicked(self):
        reductionRequest = SNAPRequest(path="calibration/reduction", payload=RunConfig(runNumber="57514").json())
        self.handleButtonClicked(reductionRequest, self.view.calibrationReductinButton)

    def handleCalibrationIndexButtonClicked(self):
        reductionRequest = SNAPRequest(
            path="calibration/save",
            payload=CalibrationIndexEntry(runNumber="57514", comments="test comment", author="test author").json(),
        )
        self.handleButtonClicked(reductionRequest, self.view.calibrationIndexButton)

    def handleButtonClicked(self, reductionRequest, button):
        button.setEnabled(False)

        # setup workers with work targets and args
        self.worker = self.worker_pool.createWorker(
            target=self.interfaceController.executeRequest, args=(reductionRequest)
        )

        # Final resets
        self.worker.finished.connect(lambda: button.setEnabled(True))

        self.worker_pool.submitWorker(self.worker)

    def handleCalibrantSampleButtonClicked(self):
        test_file_path = os.path.join(Resource._resourcesPath, "test_id123.json")
        if os.path.exists(test_file_path):
            os.remove(test_file_path)
        mat = Material(
            chemical_composition="chemicalComp", mass_density=4.4, packing_fraction=0.9, microstructure="poly-crystal"
        )
        geo = Geometry(form="cylinder", radius=3.4, illuminated_height=3.5, total_height=3.6)
        crystal = Crystallography(
            cif_file=str(os.path.join(Resource._resourcesPath, "not_real.cif")),
            space_group="outter space",
            lattice_parameters=[0, 1, 2, 3, 4, 5],
            atom_type="Na Cl",
            atom_coordinates=[0.1, 0.2, 0.3],
            site_occupation_factor=0.4,
        )
        sample = CalibrantSamples(name="test", unique_id="id123", geometry=geo, material=mat, crystallography=crystal)
        saveRequest = SNAPRequest(path="calibrant_sample/save_sample", payload=sample.json())
        self.handleButtonClicked(saveRequest, self.view.saveCalibrantButton)
