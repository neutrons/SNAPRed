from unittest import mock

import pytest

from snapred.__main__ import _preloadImports, main


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


def test_preloadImports_success(capsys):
    """Test _preloadImports function executes successfully and prints success message."""
    _preloadImports()
    captured = capsys.readouterr()
    assert "preloaded qtpy.QtWebEngineWidgets" in captured.out
    assert "SNAPRed algorithms and services registered with Mantid" in captured.out


def test_preloadImports_exception_handling(capsys):  # noqa: ARG001
    """Test _preloadImports function handles exceptions properly and prints warning."""
    # Mock an import that will raise an exception during the try block
    with mock.patch("snapred.__main__.print") as mock_print:
        # Make the first print call succeed (for the qtpy import message)
        # But make the second print call (success message) raise an exception
        # Add a third None for the warning message call
        mock_print.side_effect = [None, Exception("Test exception"), None]

        _preloadImports()

        # Verify the exception handling code was called
        expected_calls = [
            mock.call("preloaded qtpy.QtWebEngineWidgets"),
            mock.call("SNAPRed algorithms and services registered with Mantid"),
            mock.call("Warning: Failed to register SNAPRed with Mantid: Test exception"),
        ]
        mock_print.assert_has_calls(expected_calls)
