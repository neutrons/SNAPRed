

from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.indexing.Versioning import VersionState
from snapred.backend.data.LocalDataService import LocalDataService


instrumentConfigJson = {
  "version": 1.4,
  "facility": "SNS",
  "name": "SNAP",
  "nexusFileExtension": ".nxs.h5",
  "nexusFilePrefix": "SNAP_",
  "calibrationDirectory": "/SNS/SNAP/shared/Calibration/Powder/",
  "calibrationFilePrefix": "SNAPcalibLog",
  "calibrationFileExtension": "json",
  "pixelGroupingDirectory":"PixelGroupingDefinitions/",
  "sharedDirectory": "shared/",
  "nexusDirectory": "nexus/",
  "reducedDataDirectory": "shared/manualReduced/",
  "reductionRecordDirectory": "shared/manualReduced/reductionRecord/",
  "GSAS-IIDirectory": "GSAS-II/",
  "GSAS-IIExtension": ".gsa",
  "neutronBandwidth": 3.2,
  "extendedNeutronBandwidth": 3.2,
  "L1": 15.0,
  "L2": 0.5,
  "delToT": 0.002,
  "delLoL": 3.226e-3,
  "delThNoGuide": 2.00e-3,
  "delThWithGuide": 6.40e-3,
  "alpha": 0.1,
  "beta_0": 0.02,
  "beta_1": 0.05,
  "width": 1600.0,
  "frequency": 60.4
}

del instrumentConfigJson["version"]
instrumentConfigJson["bandwidth"] = instrumentConfigJson.pop("neutronBandwidth")
instrumentConfigJson["maxBandwidth"] = instrumentConfigJson.pop("extendedNeutronBandwidth")
instrumentConfigJson["delTOverT"] = instrumentConfigJson.pop("delToT")
instrumentConfigJson["delLOverL"] = instrumentConfigJson.pop("delLoL")


instrumentConfig = InstrumentConfig(
    version=VersionState.NEXT,
    **instrumentConfigJson
)


localDataService = LocalDataService()
localDataService.writeInstrumentParameters(instrumentConfig, ">=46342,<=63976", "author: snap test script")

savedInstrumentConfig = localDataService.readInstrumentParameters(46342)
print(savedInstrumentConfig.model_dump_json())