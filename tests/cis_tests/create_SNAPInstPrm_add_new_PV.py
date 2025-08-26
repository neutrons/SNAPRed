from pathlib import Path
import sys

import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent

from snapred.backend.dao.indexing.Versioning import VersionState
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.dao.state.InstrumentConfig import InstrumentConfig
from snapred.backend.data.LocalDataService import LocalDataService

sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.IPTS_override import IPTS_override

#############################################################
## If required: override the IPTS search directories:      ##
##   => if you're not using a `dev.yml` this does nothing! ##
with IPTS_override(): # defaults to `Config["IPTS.root"]`  ##
#############################################################

    instrumentConfigJson = {
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
        "bandwidth": 3.2,
        "maxBandwidth": 3.2,
        "L1": 15.0,
        "L2": 0.5,
        "delTOverT": 0.002,
        "delLOverL": 3.226e-3,
        "alpha": 0.1,
        "beta_0": 0.02,
        "beta_1": 0.05,
        "width": 1600.0,
        "frequency": 60.4,
        
        "stateIdSchema": DetectorState.LEGACY_SCHEMA,
    
         "indexEntry": {
            "version": 0,
            "runNumber": "000000",
            "useLiteMode": False
        }
    }

    ##
    ## REMINDER to ALSO add the new PV to the list in `application.yml`!
    ##
    
    # Add the new PV to the schema:
    instrumentConfigJson["stateIdSchema"]["properties"]["BL3:Mot:OpticsPos:ExitSlit"] = {"type": "integer"}
    instrumentConfigJson["stateIdSchema"]["required"].append("BL3:Mot:OpticsPos:ExitSlit")

    # Add the new 'deltaTheta' values to the schema:
    instrumentConfigJson["stateIdSchema"]["derivedPVs"]["deltaTheta"] =  {
        # the tuple of PVs used as the key (as a list):
        "keyPVs": [
            "BL3:Mot:OpticsPos:Pos",
            "BL3:Mot:OpticsPos:ExitSlit"
        ],
        # A list of key-value pairs used to form the map:
        #   the keys are in the same order as the 'keyPVs' tuple.
        ## During execution: a request for any pair NOT in this list will raise an exception. ##
        "items": [
            [[1, 0], 6.40e-3],
            [[1, 1], 2.0e-3],
            [[1, 3], 2.00e-3],
            [[2, 0], 2.0e-3],
            [[2, 1], 2.0e-3],
            [[2, 3], 2.0e-3]
        ]
    }

    instrumentConfig = InstrumentConfig(
        version=VersionState.NEXT,
        **instrumentConfigJson
    )


    localDataService = LocalDataService()
    localDataService.writeInstrumentParameters(instrumentConfig, ">=66670,<=66700", "author: snap test script")

    savedInstrumentConfig = localDataService.readInstrumentParameters(66670)
    print(savedInstrumentConfig.model_dump_json(indent=2))
