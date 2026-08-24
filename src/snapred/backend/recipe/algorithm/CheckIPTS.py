#
# Copyright &copy; 2018 ISIS Rutherford Appleton Laboratory UKRI,
#   NScD Oak Ridge National Laboratory, European Spallation Source,
#   Institut Laue - Langevin & CSNS, Institute of High Energy Physics, CAS
# SPDX - License - Identifier: GPL - 3.0 +

# Based on `<mantid repo>/Framework/PythonInterface/plugins/algorithms/GetIPTS.py` with only minor modifications.

from functools import lru_cache

from mantid.api import AlgorithmFactory, FileFinder, PythonAlgorithm
from mantid.kernel import ConfigService, Direction, IntBoundedValidator


class CheckIPTS(PythonAlgorithm):
    def category(self):
        return "Utility\\ORNL"

    def name(self):
        return "CheckIPTS"

    def summary(self):
        return "Extracts the IPTS number from a run using FileFinder, returns empty string if no such directory exists"

    # Facilities searched when no 'Facility' is named.  This algorithm is an ORNL utility (it derives
    #   from Mantid's `GetIPTS`), so these -- and not the Mantid *default* facility -- are the
    #   behavior-preserving fallback.
    DEFAULT_FACILITIES = ("SNS", "HFIR")

    def getValidInstruments(self, facilityName: str = ""):
        """Instrument short-names valid for `facilityName`, or for the ORNL facilities if unnamed.

        The empty string is included: it means "let `FileFinder` resolve the bare run number".
        """
        instruments = [""]

        facilityNames = (facilityName,) if facilityName else self.DEFAULT_FACILITIES
        for name in facilityNames:
            facility = ConfigService.getFacility(name)
            # Note: `item` is an `InstrumentInfo`, so the "DAS" exclusion must compare against its short name.
            facilityInstruments = sorted(
                [item.shortName() for item in facility.instruments() if item.shortName() != "DAS"]
            )
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
            # Note: the default instrument is *not* substituted here -- `FileFinder` is left to
            #   resolve the bare run number.  Callers within SNAPRed always pass 'Instrument'
            #   explicitly, and should continue to: relying on Mantid's process-wide default
            #   instrument makes behavior depend on unrelated user configuration.
            self.log().information(
                f"No instrument specified for run '{runnumber}': leaving resolution to `FileFinder`."
                f"  (Mantid's default instrument is '{ConfigService.getInstrument().name()}'.)"
            )

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

        # NOTE: 'Instrument' deliberately carries no `StringListValidator`.  Such a validator is built
        #   once, when the algorithm is initialized, and is never re-evaluated -- so it could not take
        #   the 'Facility' property below into account, since property values are only set *after*
        #   initialization.  The check is therefore performed in `validateInputs`, where both
        #   properties are available.  This is the same shape proposed for Mantid's live-data
        #   algorithms; see 'docs/source/developer/implementation_notes/mantid_config_ownership.rst'.
        self.declareProperty("Instrument", "", direction=Direction.Input, doc="Empty lets `FileFinder` resolve the run")

        # An instrument name is only meaningful with respect to a facility, so the two are declared
        #   together.  Empty means the ORNL facilities (see `DEFAULT_FACILITIES`).
        self.declareProperty(
            "Facility",
            "",
            direction=Direction.Input,
            doc="[optional] facility owning 'Instrument'; empty searches the ORNL facilities",
        )

        self.declareProperty("ClearCache", False, "Remove internal cache of run descriptions to file paths")

        self.declareProperty("Directory", "", direction=Direction.Output)

    def validateInputs(self) -> dict:
        errors = {}

        facilityName = self.getProperty("Facility").value
        instrumentName = self.getProperty("Instrument").value

        try:
            validInstruments = self.getValidInstruments(facilityName)
        except RuntimeError as e:
            if "Facilities search object" not in str(e):
                raise
            errors["Facility"] = f"Facility '{facilityName}' is not known to Mantid."
            return errors

        if instrumentName not in validInstruments:
            searched = facilityName if facilityName else " / ".join(self.DEFAULT_FACILITIES)
            errors["Instrument"] = f"Instrument '{instrumentName}' is not part of facility '{searched}'."

        return errors

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
