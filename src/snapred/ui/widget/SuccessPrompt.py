from qtpy.QtCore import QMetaObject, Qt, Slot
from qtpy.QtWidgets import QDialog, QLabel, QPushButton, QVBoxLayout


class SuccessPrompt(QDialog):
    """

    This qt dialog is crafted to provide users with immediate, clear feedback on successful operations,
    like confirming the successful setup of a state, aligning with GUI design principles for straightforward
    communication. It features a simple, minimalistic design with a fixed size and essential window options
    to focus user attention on the success message. The layout includes a vertically arranged message label
    and an "OK" button, ensuring easy readability and quick user acknowledgment. By prioritizing efficient
    information delivery and facilitating a smooth continuation of tasks, this dialog enhances the overall
    user experience in applications by affirming positive action outcomes without unnecessary complexity.

    """

    def __init__(self, parent=None):
        super().__init__(parent, Qt.WindowCloseButtonHint | Qt.WindowTitleHint)
        self.setWindowModality(Qt.ApplicationModal)
        self.setWindowTitle("Success")
        self.setFixedSize(300, 120)

        # Do not hide the `QWidget.layout()` method!
        layout_ = QVBoxLayout()

        label = QLabel("State initialized successfully.")
        layout_.addWidget(label)

        self.okButton = QPushButton("OK")
        layout_.addWidget(self.okButton)
        self.okButton.clicked.connect(self.accept)

        self.setLayout(layout_)

    @Slot()
    def accept(self):
        super().accept()
        if self.parent():
            # Queuing this allows "parent" to close "self".
            QMetaObject.invokeMethod(self.parent(), "close", Qt.QueuedConnection)

    # A static "factory" method to facilitate testing.
    @staticmethod
    def prompt(parent=None):
        # Use of `setWindowModality(Qt.ApplicationModal)` with `open`
        #   allows synchronous tasks.  Use of `exec_` does not, and should be avoided.
        SuccessPrompt(parent).open()
