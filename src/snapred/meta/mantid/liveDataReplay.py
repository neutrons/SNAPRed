"""
Mantid's replay live-data listener: instrument 'ADARA_FileReader' of facility 'TEST_LIVE'.

It stands in for the beamline listener when testing live-data mode without a beamline, and is selected by
SNAPRed's configuration (`liveData.facility.name`, `liveData.instrument.name`).  It reads its file and its
number of chunks from Mantid's configuration: 'fileeventdatalistener.filename' and
'fileeventdatalistener.chunks' (which the GUI sets from `liveData.testInput`).

It differs from the beamline listener in ways that SNAPRed's live-data flow has to allow for:

* it has no network address (its port is 0): it is available when its file is;
* the data is of a finished run, with that run's 'end_time': the replay itself stands for a running run;
* its chunks divide the *events* of the file, not its time: each chunk spans the whole run, so the data is
  complete only once every chunk has been read.  After extracting a chunk it starts loading the next one
  in the background, and a listener destroyed before that load has finished waits for it.
"""

from pathlib import Path
from typing import Optional

from mantid.api import FileFinder
from mantid.kernel import ConfigService

from snapred.meta.Config import Config

FACILITY = "TEST_LIVE"
INSTRUMENT = "ADARA_FileReader"
FILENAME_KEY = "fileeventdatalistener.filename"
CHUNKS_KEY = "fileeventdatalistener.chunks"


def usingFileReplay() -> bool:
    """Whether SNAPRed's live data comes from the replay listener."""
    return Config["liveData.facility.name"] == FACILITY and Config["liveData.instrument.name"] == INSTRUMENT


def replayFile() -> Optional[Path]:
    """The file that the replay listener reads, as it finds it, or `None` if it isn't set or isn't found."""
    filename = ConfigService.getString(FILENAME_KEY)
    if not filename:
        return None
    path = FileFinder.getFullPath(filename)
    return Path(path) if path and Path(path).exists() else None


def replayChunks() -> int:
    """The number of chunks that the replay listener reads the file in."""
    try:
        return max(1, int(ConfigService.getString(CHUNKS_KEY)))
    except ValueError:
        return 1
