import sys

from snapred import __version__ as snapred_version
from snapred.backend.shims import amend_mantid_config
from snapred.meta.Config import Resource


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


def main(args=None):
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
    options = parser.parse_args(args)

    # fix up some of the options for mantid
    options.checkfornewmantid = _bool_to_mtd_str(options.checkfornewmantid)
    options.updateinstruments = _bool_to_mtd_str(options.updateinstruments)
    options.reportusage = _bool_to_mtd_str(options.reportusage)

    # show the ascii splash screen
    _print_text_splash()

    # start the gui
    with amend_mantid_config(
        new_config={
            "CheckMantidVersion.OnStartup": options.checkfornewmantid,
            "UpdateInstrumentDefinitions.OnStartup": options.updateinstruments,
            "usagereports.enabled": options.reportusage,
        }
    ):
        from snapred.ui.main import start

        return start(options)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
