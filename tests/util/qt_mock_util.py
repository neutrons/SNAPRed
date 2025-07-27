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
    def exec(msg, moduleRoot=_moduleRoot):
        myCounterMock = mock.Mock()

        def _mockExec(self_):
            myCounterMock(self_)
            return (
                QMessageBox.Ok
                if (msg in self_.detailedText())
                else (
                    MockQMessageBox().fail(
                        f"Expected warning not found:  {msg}:\n"
                        + f"  actual message: {self_.text()};\n"
                        + f"  actual details: {self_.detailedText()}"
                    )
                ),
            )

        return mock.patch(moduleRoot + ".QMessageBox.exec", _mockExec), myCounterMock

    @staticmethod
    def critical(msg: str, moduleRoot=_moduleRoot):  # noqa: ARG004
        myCounterMock = mock.Mock()

        def _mockCritical(*args):
            myCounterMock(*args)
            return (
                QMessageBox.Ok
                if msg in args[2]
                else (MockQMessageBox().fail(f"Expected error does not match: {args[2]}")),
            )

        return mock.patch(moduleRoot + ".QMessageBox.critical", _mockCritical), myCounterMock

    @staticmethod
    def warning(msg: str, moduleRoot=_moduleRoot):  # noqa: ARG004
        myCounterMock = mock.Mock()

        def _mockWarning(*args, **kwargs):
            myCounterMock(*args, **kwargs)
            return (
                QMessageBox.Ok
                if msg in args[2]
                else (MockQMessageBox().fail(f"Expected error does not match: {args[2]}")),
            )

        return mock.patch(moduleRoot + ".QMessageBox.warning", _mockWarning), myCounterMock

    @staticmethod
    def continueWarning(msg, moduleRoot=_moduleRoot):
        myCounterMock = mock.Mock()

        def _mockExec(self_):
            if msg in self_.text():
                myCounterMock(self_)
                return QMessageBox.Yes
            elif "No valid FocusGroups were specified for mode: 'lite'" in self_.detailedText():
                # TODO: Please fix the input data.  Doing this is really hacky!
                return QMessageBox.Yes
            MockQMessageBox().fail(
                f"Expected warning not found:  {msg}:\n"
                + f"  actual message: {self_.text()};\n"
                + f"  actual details: {self_.detailedText()}"
            )
            
        return mock.patch(moduleRoot + ".QMessageBox.exec", _mockExec), myCounterMock

    @staticmethod
    def continueButton(buttonText, moduleRoot=_moduleRoot):
        myCounterMock = mock.Mock()

        def _mockButton(self_):
            myCounterMock(self_)

            def text(self):  # noqa: ARG001
                return buttonText

            myCounterMock.side_effect = text
            return myCounterMock

        return mock.patch(moduleRoot + ".QMessageBox.clickedButton", _mockButton), myCounterMock
