from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QDialog, QLabel, QPushButton, QVBoxLayout


class SuccessDialog(QDialog):
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
