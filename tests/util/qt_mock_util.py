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
    def exec(self, ref, msg: str):  # noqa: ARG004
        return mock.patch(
            "qtpy.QtWidgets.QMessageBox.exec",
            lambda ref, *args, **kwargs: QMessageBox.Ok  # noqa: ARG005
            if (
                "The backend has encountered warning(s)" in ref.text()
                and "Run number -2 must be a positive integer" in ref.detailedText()
            )
            else (self.fail(msg)),
        )
