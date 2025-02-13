from unittest import mock

from qtpy.QtCore import Slot
from qtpy.QtWidgets import (
    QMessageBox,
    QWidget,
)


class MockQMessageBox(QWidget):
    @Slot(str)
    def fail(self, msg: str):
        print(msg)
        raise Exception(msg)

    @staticmethod
    def exec(msg):
        myCounterMock = mock.Mock()

        def _mockExec(self_):
            myCounterMock(self_)
            return (
                QMessageBox.Ok
                # if ("The backend has encountered warning(s)" in self_.text() and msg in self_.detailedText())
                if (msg in self_.detailedText())
                else (MockQMessageBox().fail(f"Expected warning not found:  {msg}")),
            )

        return mock.patch("qtpy.QtWidgets.QMessageBox.exec", _mockExec), myCounterMock

    @staticmethod
    def critical(msg: str):  # noqa: ARG004
        myCounterMock = mock.Mock()

        def _mockCritical(*args):
            myCounterMock(*args)
            return (
                QMessageBox.Ok
                if msg in args[2]
                else (MockQMessageBox().fail(f"Expected error does not match: {args[2]}")),
            )

        return mock.patch("qtpy.QtWidgets.QMessageBox.critical", _mockCritical), myCounterMock

    @staticmethod
    def warning(msg: str):  # noqa: ARG004
        myCounterMock = mock.Mock()

        def _mockWarning(*args, **kwargs):
            myCounterMock(*args, **kwargs)
            return (
                QMessageBox.Ok
                if msg in args[2]
                else (MockQMessageBox().fail(f"Expected error does not match: {args[2]}")),
            )

        return mock.patch("qtpy.QtWidgets.QMessageBox.warning", _mockWarning), myCounterMock

    @staticmethod
    def continueWarning(msg: str):  # noqa: ARG004
        myCounterMock = mock.Mock()

        def _mockWarning(*args, **kwargs):
            myCounterMock(*args, **kwargs)
            return (
                QMessageBox.Yes
                if msg in args[2]
                else (MockQMessageBox().fail(f"Expected error:\n {msg} does not match:\n {args[2]}"))
            )

        return mock.patch("qtpy.QtWidgets.QMessageBox.warning", _mockWarning), myCounterMock
