import sys

from snapred import __version__ as snapred_version
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


def main(args=None):
    import argparse

    parser = argparse.ArgumentParser(
        prog="snapred", description="Data reduction software for SNAP", epilog="https://snapred.readthedocs.io/"
    )
    parser.add_argument("-v", "--version", action="version", version=snapred_version)
    parser.parse_args(args)

    # show the ascii splash screen
    _print_text_splash()

    # start the gui
    from snapred.ui.main import start

    return start()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
