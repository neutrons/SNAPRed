"""
Run ``livereduce`` (https://github.com/mantidproject/livereduce) on a replayed NeXus file, with the
SNAPRed post-processing script in this directory.

    pixi run python tests/cis_tests/livereduce/run_livereduce.py \\
        --livereduce <livereduce checkout>/scripts/livereduce.py --nexus <SNAP_xxxxx.nxs.h5> [options]

* The NeXus file is replayed by Mantid's `FileEventDataListener` (facility 'TEST_LIVE', instrument
  'ADARA_FileReader'), in `--chunks` chunks.  It must be a raw event file ('/entry'), not processed NeXus.
* livereduce does not set the listener's `fileeventdatalistener.*` properties: this launcher sets them,
  then runs `livereduce.py` in this process, so that the post-processing script runs with the same
  `ConfigService` and SNAPRed `Config`.
* `--ipts-root` points SNAPRed at another data tree (e.g. 'tests/data/snapred-data/SNS'); by default
  the `IPTS.root` from SNAPRed's configuration ('/SNS') is used.  The reduction home is always a
  temporary directory: nothing is saved.
* The livereduce configuration is written to `--workdir`, where livereduce also writes 'livereduce.log'.

livereduce restarts the listener when it stops: once the file has been read, stop it with Ctrl-C.
"""

import argparse
import json
import runpy
import sys
import tempfile
from contextlib import ExitStack
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--livereduce", required=True, type=Path, help="path to livereduce's 'scripts/livereduce.py'")
    parser.add_argument("--nexus", required=True, type=Path, help="raw event NeXus file to replay")
    parser.add_argument("--chunks", type=int, default=3, help="number of chunks the file is replayed in")
    parser.add_argument("--update-every", type=int, default=30, help="livereduce 'update_every' (seconds)")
    parser.add_argument("--ipts-root", type=Path, default=None, help="SNAPRed 'IPTS.root' override")
    parser.add_argument("--workdir", type=Path, default=Path.cwd(), help="where the config and log are written")
    args = parser.parse_args()

    sys.path[:0] = [str(HERE), str(REPO / "tests")]

    from mantid.kernel import ConfigService
    from util.Config_helpers import Config_override
    from util.IPTS_override import IPTS_override

    ConfigService.setString("fileeventdatalistener.filename", str(args.nexus.resolve()))
    ConfigService.setString("fileeventdatalistener.chunks", str(args.chunks))

    args.workdir.mkdir(parents=True, exist_ok=True)
    configPath = args.workdir / "snap_file_replay.conf"
    configPath.write_text(
        json.dumps(
            {
                "instrument": "ADARA_FileReader",
                "script_dir": str(HERE),
                "update_every": args.update_every,
                "accum_method": "Add",
                "preserve_events": True,
            },
            indent=2,
        )
    )
    print(f"livereduce configuration: {configPath}\n{configPath.read_text()}")

    with ExitStack() as stack:
        if args.ipts_root is not None:
            stack.enter_context(IPTS_override(str(args.ipts_root.resolve())))
        reductionHome = stack.enter_context(tempfile.TemporaryDirectory(prefix="snapred_livereduce_"))
        stack.enter_context(Config_override("instrument.reduction.home", reductionHome))

        sys.argv = [str(args.livereduce), str(configPath)]
        runpy.run_path(str(args.livereduce), run_name="__main__")


if __name__ == "__main__":
    main()
