from snapred.backend.dao.state.CalibrantSample.CalibrantSample import CalibrantSample
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class CalibrantSampleService(Service):
    def __init__(self):
        super().__init__()

        # 'DataExportService' is a singleton:
        #   declaring it as an instance attribute, instead of a class attribute,
        #   allows singleton reset during testing.
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
