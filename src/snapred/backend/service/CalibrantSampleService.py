from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class CalibrantSampleService(Service):
    _name = "calibrant_sample"
    dataExportService = DataExportService()

    def name(self):
        return self._name

    @FromString
    def save_sample(self, calibrantSample: CalibrantSamples):
        try:
            self.dataExportService(calibrantSample)
        except:
            raise
