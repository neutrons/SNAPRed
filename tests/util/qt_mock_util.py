from unittest import mock

import pytest
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
    def exec(self, ref, msg: str):  # noqa: ARG004
        return mock.patch(
            "qtpy.QtWidgets.QMessageBox.exec",
            lambda ref, *args, **kwargs: QMessageBox.Ok  # noqa: ARG005
            if ("The backend has encountered warning(s)" in ref.text() and msg in ref.detailedText())
            else (self.fail(f"Expected error does not match: {msg}")),
        )

    @staticmethod
    def mockExec(_self, msg):
        def _myMessageBoxExecMock(*args, **kwargs):
            return (
                QMessageBox.Ok
                if ("The backend has encountered warning(s)" in _self.text() and msg in _self.detailedText())
                else pytest.fail(
                    "unexpected QMessageBox.exec:"
                    + f"    args: {args}"
                    + f"    kwargs: {kwargs}"
                    + f"    text: '{_self.text()}'"
                    + f"    detailed text: '{_self.detailedText()}'",
                    pytrace=False,
                )
            )

        return mock.patch("qtpy.QtWidgets.QMessageBox.exec", side_effect=_myMessageBoxExecMock)

    @staticmethod
    def critical(self, msg: str):  # noqa: ARG004
        return mock.patch(
            "qtpy.QtWidgets.QMessageBox.critical",
            lambda *args, **kwargs: QMessageBox.Ok  # noqa: ARG005
            if msg in args[2]
            else (self.fail(f"Expected error does not match: {msg}")),
        )

    @staticmethod
    def warning(self, msg: str):  # noqa: ARG004
        return mock.patch(
            "qtpy.QtWidgets.QMessageBox.warning",
            lambda *args, **kwargs: QMessageBox.Ok  # noqa: ARG005
            if msg in args[2]
            else (self.fail(f"Expected error does not match: {msg}")),
        )

    @staticmethod
    def warningNo(self, msg: str):  # noqa: ARG004
        return mock.patch(
            "qtpy.QtWidgets.QMessageBox.warning",
            lambda *args, **kwargs: QMessageBox.No  # noqa: ARG005
            if msg in args[2]
            else (self.fail(f"Expected error does not match: {msg}")),
        )
