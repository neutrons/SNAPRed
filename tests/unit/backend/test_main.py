from unittest import mock

import pytest

from snapred.__main__ import main


@pytest.mark.parametrize("option", ["-h", "--help", "-v", "--version"])
def test_simple(option):
    with pytest.raises(SystemExit):
        main([option])


def test_workbench():
    with (
        mock.patch("snapred.__main__.workbench_start") as mockStart,
        mock.patch("snapred.__main__._print_text_splash"),
        mock.patch("snapred.__main__.os") as mockOs,
    ):
        mockOs.fork.return_value = 0
        main(["--workbench"])
        mockOs.fork.assert_called_once()
        mockStart.assert_called_once()

        mockStart.reset_mock()
        mockOs.fork.return_value = 1
        main(["--workbench"])
        mockStart.assert_not_called()


def test_configure():
    with (
        mock.patch("snapred.__main__.Config.configureForDeploy") as mockConfigure,
        mock.patch("snapred.__main__._print_text_splash"),
    ):
        main(["--configure"])
        mockConfigure.assert_called_once()
