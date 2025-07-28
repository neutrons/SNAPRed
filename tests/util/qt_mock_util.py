import re
from unittest import mock

from qtpy.QtCore import Slot
from qtpy.QtWidgets import (
    QMessageBox,
    QWidget,
)


class MockQMessageBox(QWidget):
    _moduleRoot = "qtpy.QtWidgets"

    @Slot(str)
    def fail(self, msg: str):
        print(msg)
        raise Exception(msg)

    @staticmethod
    def exec(text_regex: str, detailedText_regex: str | None = None, moduleRoot: str = _moduleRoot):
        _counterMock = mock.Mock()
        _textPattern = re.compile(text_regex)
        _detailedTextPattern = re.compile(detailedText_regex) if bool(detailedText_regex) else None

        def _mockExec(self_):
            _counterMock(self_)
            match = _textPattern.match(self_.text()) and (
                not bool(_detailedTextPattern) or _detailedTextPattern.match(self_.detailedText())
            )
            return (
                QMessageBox.Ok
                if match
                else (
                    MockQMessageBox().fail(
                        f"Expected warning not found:\n"
                        f"    expected text (regex): '{_textPattern.pattern}'\n"
                        f"    expected details: "
                        f"'{_detailedTextPattern.pattern if bool(_detailedTextPattern) else None}'\n"
                        f"    actual text: '{self_.text()}'\n"
                        f"    actual details: '{self_.detailedText()}'."
                    )
                ),
            )

        return mock.patch(moduleRoot + ".QMessageBox.exec", _mockExec), _counterMock

    @staticmethod
    def critical(msg_regex: str, moduleRoot: str = _moduleRoot):  # noqa: ARG004
        _counterMock = mock.Mock()
        _msgPattern = re.compile(msg_regex)

        def _mockCritical(*args):
            _counterMock(*args)
            return (
                QMessageBox.Ok
                if _msgPattern.match(args[2])
                else (
                    MockQMessageBox().fail(
                        f"Expected warning not found:\n"
                        f"    expected text (regex): '{_msgPattern.pattern}'\n"
                        f"    actual text: '{args[2]}'."
                    )
                ),
            )

        return mock.patch(moduleRoot + ".QMessageBox.critical", _mockCritical), _counterMock

    @staticmethod
    def warning(msg_regex: str, moduleRoot: str = _moduleRoot):  # noqa: ARG004
        _counterMock = mock.Mock()
        _msgPattern = re.compile(msg_regex)

        def _mockWarning(*args, **kwargs):
            _counterMock(*args, **kwargs)
            return (
                QMessageBox.Ok
                if _msgPattern.match(args[2])
                else (
                    MockQMessageBox().fail(
                        f"Expected warning not found:\n"
                        f"    expected text (regex): '{_msgPattern.pattern}'\n"
                        f"    actual text: '{args[2]}'."
                    )
                ),
            )

        return mock.patch(moduleRoot + ".QMessageBox.warning", _mockWarning), _counterMock

    @staticmethod
    def continueWarning(msg_regex: str, moduleRoot: str = _moduleRoot):
        return MockQMessageBox.exec(text_regex=msg_regex, moduleRoot=moduleRoot)

    @staticmethod
    def continueButton(buttonText: str, moduleRoot: str = _moduleRoot):
        _counterMock = mock.Mock()

        def _mockButton(self_):
            _counterMock(self_)

            def text(self_):  # noqa: ARG001
                return buttonText

            _counterMock.side_effect = text
            return _counterMock

        return mock.patch(moduleRoot + ".QMessageBox.clickedButton", _mockButton), _counterMock
