from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QDialog, QLabel, QPushButton, QVBoxLayout


class SuccessDialog(QDialog):
    """
    A PyQt5 dialog designed to succinctly inform users of successful operations, specifically
    tailored for instances such as confirming the successful initialization of a state. This class
    adheres to GUI design best practices by delivering clear, concise feedback to users, enhancing
    the user experience by affirming positive outcomes of operations.

    Key Features and Functionality:

    - Window Options: Employs Qt.WindowCloseButtonHint and Qt.WindowTitleHint to present a focused,
      minimalistic dialog emphasizing the message.
    - Fixed Size: Maintains a consistent display across various platforms with a set size of
      300x120 pixels, optimizing readability and interface consistency.

    Layout and Content:

    - Vertical Layout: Utilizes QVBoxLayout for an efficient, linear arrangement of components
      including a message label and an acknowledgment button.
    - Message Label: Conveys the success message ("State initialized successfully.") directly,
      prioritizing clear communication with the user.
    - OK Button: Facilitates dialog closure and user acknowledgment with a single action button.
      Connected to the accept method, it ensures a seamless exit from the dialog, including the
      closure of the parent window if applicable, streamlining the user's progression through tasks.

    This class embodies a streamlined approach to user feedback in GUI applications, providing
    essential information with minimal distraction, thereby fostering a positive user experience.

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
