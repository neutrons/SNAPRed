from qtpy.QtCore import Qt
from qtpy.QtWidgets import QDialog, QLabel, QPushButton, QVBoxLayout


class SuccessDialog(QDialog):
    """

    This PyQt5 dialog is crafted to provide users with immediate, clear feedback on successful operations,
    like confirming the successful setup of a state, aligning with GUI design principles for straightforward
    communication. It features a simple, minimalistic design with a fixed size and essential window options
    to focus user attention on the success message. The layout includes a vertically arranged message label
    and an "OK" button, ensuring easy readability and quick user acknowledgment. By prioritizing efficient
    information delivery and facilitating a smooth continuation of tasks, this dialog enhances the overall
    user experience in applications by affirming positive action outcomes without unnecessary complexity.

    """

    def __init__(self, parent=None):
        super().__init__(parent, Qt.WindowCloseButtonHint | Qt.WindowTitleHint)
        self.setWindowTitle("Success")
        self.setFixedSize(300, 120)
        layout = QVBoxLayout()

        label = QLabel("State initialized successfully.")
        layout.addWidget(label)

        okButton = QPushButton("OK")
        layout.addWidget(okButton)
        okButton.clicked.connect(self.accept)

        self.setLayout(layout)

    def accept(self):
        super().accept()
        if self.parent():
            self.parent().close()
