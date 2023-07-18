import sys

from snapred.meta.Config import Resource


def main(args=None):  # noqa: ARG001
    # ex.resize(700, 700)
    asciiPath = "ascii.txt"
    with Resource.open(asciiPath, "r") as asciiArt:
        artString = asciiArt.read().split("\n")
        ornlLogo = artString[:33]
        snapRedText = artString[33:]
        for value, line in enumerate(ornlLogo):
            print("\033[38:2:8:{}:{}:{}m{}\033[00m".format(0, 60 + value * 4, value * 2, line))
        for value, line in enumerate(snapRedText):
            print("\033[38:2:0:136:{}:{}m {}\033[00m".format(value * 4 + 51, value * 4 + 46, line))

    # start the gui
    from snapred.ui.main import start

    return start()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
