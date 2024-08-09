from snapred.backend.dao.state.CalibrantSample.CalibrantSample import CalibrantSample
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class CalibrantSampleService(Service):
    dataExportService: "DataExportService"

    def __init__(self):
        super().__init__()
        self.dataExportService = DataExportService()
        self.registerPath("save_sample", self.save_sample)
        return

    @staticmethod
    def name():
        return "calibrant_sample"

    @FromString
    def save_sample(self, calibrantSample: CalibrantSample):
        try:
            self.dataExportService.exportCalibrantSampleFile(calibrantSample)
        except:
            raise
