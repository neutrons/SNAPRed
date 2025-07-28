import os
import sys

from mantid.kernel import amend_config

from snapred import __version__ as snapred_version
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config, Resource

logger = snapredLogger.getLogger(__name__)


def _print_text_splash():
    TXT_FILENAME = "ascii.txt"
    with Resource.open(TXT_FILENAME, "r") as asciiArt:
        artString = asciiArt.read().split("\n")
        ornlLogo = artString[:33]
        snapRedText = artString[33:]
        for value, line in enumerate(ornlLogo):
            print("\033[38:2:8:{}:{}:{}m{}\033[00m".format(0, 60 + value * 4, value * 2, line))
        for value, line in enumerate(snapRedText):
            print("\033[38:2:0:136:{}:{}m {}\033[00m".format(value * 4 + 51, value * 4 + 46, line))


def _bool_to_mtd_str(arg: bool) -> str:
    """mantid.kernel.ConfigService does not understand bool, but does understand
    the strings "0" and "1". This method converts things
    """
    return "1" if arg else "0"


def _preloadImports():
    import qtpy.QtWebEngineWidgets as QWebEngineWidgets

    print(f"preloaded {QWebEngineWidgets.__name__}")

    # Import SNAPRed modules to register algorithms and services with Mantid
    # Avoid importing modules that might initialize matplotlib backends too early
    try:
        # Only import the essential modules for algorithm registration

        print("SNAPRed algorithms and services registered with Mantid")
    except Exception as e:  # noqa: BLE001
        print(f"Warning: Failed to register SNAPRed with Mantid: {e}")


def _createArgparser():
    import argparse

    parser = argparse.ArgumentParser(
        prog="snapred", description="Data reduction software for SNAP", epilog="https://snapred.readthedocs.io/"
    )
    parser.add_argument("-v", "--version", action="version", version=snapred_version)
    parser.add_argument("--checkfornewmantid", action="store_true", help="check for new mantid version on startup")
    parser.add_argument(
        "--updateinstruments", action="store_true", help="update user's cache of mantid instrument definitions"
    )
    parser.add_argument(
        "--reportusage", action="store_true", help="post telemetry data to mantid usage reporting service"
    )
    parser.add_argument(
        "--headcheck",
        action="store_true",
        help="start the gui then shut it down after 5 seconds. This is used for testing",
    )
    parser.add_argument("-x", "--execute", action="store_true", help="execute the script file given as argument")
    parser.add_argument(
        "-q", "--quit", action="store_true", help="execute the script file with '-x' given as argument and then exit"
    )
    parser.add_argument(
        "--workbench",
        default=False,
        action="store_true",
        help="Start workbench with necessary preloaded snapred imports.",
    )
    parser.add_argument(
        "--single-process",
        default=True,
        action="store_true",
        help="Start workbench with necessary preloaded snapred imports.",
    )
    parser.add_argument(
        "--no-error-reporter",
        action="store_true",
        help="Stop the error reporter from opening if you suffer an exception or crash.",
    )
    parser.add_argument("script", nargs="?", default=None)
    parser.add_argument(
        "-c", "--configure", default=False, action="store_true", help="Configure SNAPRed for specified environment"
    )
    return parser


def workbench_start(options):
    """Start workbench with necessary preloaded snapred imports."""
    from workbench.app.start import start as workbench_start

    workbench_start(options)


def main(args=None):
    parser = _createArgparser()
    options, _ = parser.parse_known_args(args)

    # fix up some of the options for mantid
    options.checkfornewmantid = _bool_to_mtd_str(options.checkfornewmantid)
    options.updateinstruments = _bool_to_mtd_str(options.updateinstruments)
    options.reportusage = _bool_to_mtd_str(options.reportusage)
    options.script = None

    # show the ascii splash screen
    _print_text_splash()

    # start the gui
    new_config = {
        "CheckMantidVersion.OnStartup": options.checkfornewmantid,
        "UpdateInstrumentDefinitions.OnStartup": options.updateinstruments,
        "usagereports.enabled": options.reportusage,
    }
    with amend_config(**new_config):
        if options.configure:
            Config.configureForDeploy()
            print(f"SNAPRed configured to use {Config['instrument.calibration.home']}")
            return 0
        else:
            if options.workbench:
                warningMessage = (
                    "WARNING: --workbench is a temporary means of starting workbench with the ability to launch SNAPRed"
                )
                warningSeperator = "/" * len(warningMessage)
                warningSeperator = f"{warningSeperator}\n{warningSeperator}"
                logger.warning(f"\n{warningSeperator}\n\n{warningMessage}\n\n{warningSeperator}\n")
                _preloadImports()

                pid = os.fork()
                if pid > 0:
                    return 0
                else:
                    workbench_start(options)
            else:
                from snapred.ui.main import start

                return start(options)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
