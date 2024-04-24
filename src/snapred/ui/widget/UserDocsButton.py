from PyQt5.QtCore import QUrl
from PyQt5.QtWebEngineWidgets import QWebEngineView
from PyQt5.QtWidgets import QHBoxLayout, QPushButton, QWidget

from snapred.meta.Config import Config


class UserDocsButton(QWidget):
    def __init__(self, parent=None):
        super(UserDocsButton, self).__init__(parent)
        self.setStyleSheet("background-color: #F5E9E2;")
        self.initUI()

    def initUI(self):
        # Layout setup
        layout = QHBoxLayout(self)
        self.setLayout(layout)

        # Button to launch the web view
        self.button = QPushButton("User Documentation", self)
        self.button.setStyleSheet("background-color: #F5E9E2;")
        self.button.clicked.connect(self.launchWebView)
        layout.addWidget(self.button)

    def launchWebView(self):
        # Create and configure the web view
        self.webView = QWebEngineView()
        self.webView.setWindowTitle("Documentation")
        self.webView.resize(800, 600)

        # Point to the specific file on the filesystem
        url = QUrl.fromLocalFile(str(Config["docs.user.path"]))
        breakpoint()
        self.webView.setUrl(url)

        # Show the web view
        self.webView.show()
