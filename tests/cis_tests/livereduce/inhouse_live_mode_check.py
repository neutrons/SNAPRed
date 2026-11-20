"""
SNAPRed's own live-data mode (not livereduce), as the GUI drives it, with the live-data listener replaced by
Mantid's replay listener (facility 'TEST_LIVE', instrument 'ADARA_FileReader').

    pixi run python tests/cis_tests/livereduce/inhouse_live_mode_check.py <SNAP_xxxxx.nxs.h5> \
        [--chunks N] [--reductions N]

It makes the GUI's requests: 'reduction/hasLiveDataConnection', 'reduction/getLiveMetadata', then
'reduction/' with `liveDataMode`, `--reductions` times.  For each reduction it prints the calibration and
normalization used, the number of events reduced, a checksum of each reduced workspace (sums of X, Y and
Y^2: compare two versions of SNAPRed, or two numbers of chunks, with these), and the ADS.  Nothing is saved.

The replay is configured as for the GUI: `liveData.facility.name` and `liveData.instrument.name` select the
replay listener, and `liveData.testInput.inputFilename` and `liveData.testInput.chunks` are copied to
Mantid's 'fileeventdatalistener.*' settings, as `snapred.ui.main` does.
"""

import argparse
import datetime
import faulthandler
import sys
import tempfile
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("nexus", type=Path, help="raw event NeXus file to replay")
    parser.add_argument("--chunks", type=int, default=3, help="number of chunks the file is replayed in")
    parser.add_argument("--reductions", type=int, default=2, help="number of live reductions")
    args = parser.parse_args()

    # If it hangs anyway, show where.
    faulthandler.dump_traceback_later(120, repeat=True)
    sys.path[:0] = [str(REPO / "tests")]

    from mantid.kernel import ConfigService
    from mantid.simpleapi import mtd
    from util.Config_helpers import Config_override

    from snapred.backend.api.InterfaceController import InterfaceController
    from snapred.backend.dao.request.ReductionRequest import ReductionRequest
    from snapred.backend.dao.SNAPRequest import SNAPRequest
    from snapred.meta.Config import Config
    from snapred.meta.mantid import liveDataReplay

    dataSourceLog = f"{Config['metadata.tagPrefix']}dataSource"

    def request(path, payload=None):
        return InterfaceController().executeRequest(SNAPRequest(path=path, payload=payload))

    ok = True
    with (
        tempfile.TemporaryDirectory(prefix="snapred_inhouse_live_") as home,
        Config_override("instrument.reduction.home", home),
        Config_override("liveData.facility.name", liveDataReplay.FACILITY),
        Config_override("liveData.instrument.name", liveDataReplay.INSTRUMENT),
        Config_override("liveData.testInput.inputFilename", str(args.nexus.resolve())),
        Config_override("liveData.testInput.chunks", args.chunks),
    ):
        # As `snapred.ui.main` does, for the replay listener.
        ConfigService.setString(liveDataReplay.FILENAME_KEY, Config["liveData.testInput.inputFilename"])
        ConfigService.setString(liveDataReplay.CHUNKS_KEY, str(Config["liveData.testInput.chunks"]))

        response = request("reduction/hasLiveDataConnection")
        print(f"hasLiveDataConnection: {response.code} {response.data}", flush=True)
        response = request("reduction/getLiveMetadata")
        metadata = response.data
        print(f"live metadata: {response.code}: run {metadata.runNumber}, state {metadata.stateId.hex}", flush=True)

        for i in range(args.reductions):
            timestamp = request("reduction/getUniqueTimestamp").data
            response = request(
                "reduction/",
                ReductionRequest(
                    runNumber=str(metadata.runNumber),
                    useLiteMode=True,
                    timestamp=timestamp,
                    liveDataMode=True,
                    liveDataDuration=datetime.timedelta(seconds=0),
                ),
            )
            print(f"reduction {i}: {response.code} {response.message or ''}", flush=True)
            if response.code != 200:
                ok = False
                continue
            record = response.data.record
            for name, r in (("calibration", record.calibration), ("normalization", record.normalization)):
                print(f"  {name}: {(r.runNumber, r.version) if r is not None else None}", flush=True)
            for name in record.workspaceNames:
                ws = mtd[name]
                y = np.concatenate([ws.readY(j) for j in range(ws.getNumberHistograms())])
                x = np.concatenate([ws.readX(j) for j in range(ws.getNumberHistograms())])
                run = ws.getRun()
                source = run.getProperty(dataSourceLog).value if run.hasProperty(dataSourceLog) else None
                print(
                    f"  {name}: dataSource {source}, {ws.getNumberHistograms()} spectra;"
                    f" sum(Y) {np.nansum(y):.10g}, sum(Y^2) {np.nansum(y * y):.10g}, sum(X) {np.sum(x):.10g}",
                    flush=True,
                )
            inputWorkspace = f"tof_all_lite_{int(metadata.runNumber):06d}"
            if mtd.doesExist(inputWorkspace):
                print(f"  events reduced: {mtd[inputWorkspace].getNumberEvents()}", flush=True)
            print(f"  ADS after reduction {i}:", flush=True)
            for name in sorted(mtd.getObjectNames()):
                ws = mtd[name]
                events = ws.getNumberEvents() if hasattr(ws, "getNumberEvents") else ""
                print(f"    {name:55s} {ws.id():22s} {events}", flush=True)

    print("ALL REDUCTIONS OK" if ok else "FAILED", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
