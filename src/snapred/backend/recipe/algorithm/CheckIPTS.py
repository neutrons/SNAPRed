#
# Copyright &copy; 2018 ISIS Rutherford Appleton Laboratory UKRI,
#   NScD Oak Ridge National Laboratory, European Spallation Source,
#   Institut Laue - Langevin & CSNS, Institute of High Energy Physics, CAS
# SPDX - License - Identifier: GPL - 3.0 +

# Based on `<mantid repo>/Framework/PythonInterface/plugins/algorithms/GetIPTS.py` with only minor modifications.

from functools import lru_cache

from mantid.api import AlgorithmFactory, FileFinder, PythonAlgorithm
from mantid.kernel import ConfigService, Direction, IntBoundedValidator, StringListValidator


class CheckIPTS(PythonAlgorithm):
    def category(self):
        return "Utility\\ORNL"

    def name(self):
        return "CheckIPTS"

    def summary(self):
        return "Extracts the IPTS number from a run using FileFinder, returns empty string if no such directory exists"

    def getValidInstruments(self):
        instruments = [""]

        for name in ["SNS", "HFIR"]:
            facility = ConfigService.getFacility(name)
            facilityInstruments = sorted([item.shortName() for item in facility.instruments() if item != "DAS"])
            instruments.extend(facilityInstruments)

        return instruments

    @lru_cache
    @staticmethod
    def findFile(instrument, runnumber) -> str | None:
        """Static method to get the path for an instrument/runnumber.
        This assumes that within the runtime of mantid the mapping will be consistent.

        The lru_cache will allow for skipping this function if the same run number is supplied"""
        # start with run and check the five before it
        runIds = list(range(runnumber, runnumber - 6, -1))
        # check for one after as well
        runIds.append(runnumber + 1)

        runIds = [str(runId) for runId in runIds if runId > 0]

        # prepend non-empty instrument name for FileFinder
        if len(instrument) > 0:
            runIds = ["%s_%s" % (instrument, runId) for runId in runIds]

        # look for a file
        filePath = None
        for runId in runIds:
            # use filefinder to look
            try:
                filePath = FileFinder.findRuns(runId)[0]
                break
            except RuntimeError:
                pass  # just keep looking

        # Modified from `GetIPTS`: failed to find any returns None
        return filePath

    def checkIPTSLocal(self, instrument, runnumber) -> str | None:
        # prepend non-empty instrument name for FileFinder
        if len(instrument) == 0:
            instrument_default = ConfigService.getInstrument().name()
            self.log().information(f"Using default instrument: {instrument_default}")

        filename = __class__.findFile(instrument, runnumber)

        direc = None
        if bool(filename):
            # convert to the path to the proposal
            location = filename.find("IPTS")
            if location <= 0:
                raise RuntimeError("Failed to determine IPTS directory " + "from path '%s'" % filename)
            location = filename.find("/", location)
            direc = filename[0 : location + 1]
        return direc

    def PyInit(self):
        self.declareProperty(
            "RunNumber",
            defaultValue=0,
            direction=Direction.Input,
            validator=IntBoundedValidator(lower=1),
            doc="Extracts the IPTS number for a run",
        )

        instruments = self.getValidInstruments()
        self.declareProperty("Instrument", "", StringListValidator(instruments), "Empty uses default instrument")
        self.declareProperty("ClearCache", False, "Remove internal cache of run descriptions to file paths")

        self.declareProperty("Directory", "", direction=Direction.Output)

    def PyExec(self):
        instrument = self.getProperty("Instrument").value
        runnumber = self.getProperty("RunNumber").value

        if self.getProperty("ClearCache").value:
            # drop the local cache of file information
            self.findFile.cache_clear()

        direc = self.checkIPTSLocal(instrument, runnumber)
        self.setPropertyValue("Directory", direc if direc is not None else "")
        if bool(direc):
            self.log().notice(f"IPTS directory for run '{runnumber}' is: '{direc}'")
        else:
            self.log().notice(f"No IPTS directory exists for run '{runnumber}'")


AlgorithmFactory.subscribe(CheckIPTS)
