from qtpy.QtCore import QSize, Qt
from qtpy.QtGui import QPalette
from qtpy.QtWidgets import QHBoxLayout, QLabel, QStyle, QToolButton, QWidget


class ToolBarView(QWidget):
    clickPos = None

    def __init__(self, parent):
        super(ToolBarView, self).__init__(parent)
        self.setAutoFillBackground(True)

        self.setBackgroundRole(QPalette.Shadow)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(1, 1, 1, 1)
        layout.addStretch()

        self.title = QLabel("My Own Bar", self, alignment=Qt.AlignCenter)
        self.title.setObjectName("headerTitle")

        self.setObjectName("header")
        # if setPalette() was used above, this is not required
        self.title.setForegroundRole(QPalette.Light)

        style = self.style()
        ref_size = self.fontMetrics().height()
        ref_size += style.pixelMetric(style.PM_ButtonMargin) * 2
        self.setMaximumHeight(ref_size + 2)

        btn_size = QSize(ref_size, ref_size)
        for target in ("min", "normal", "max", "close"):
            btn = QToolButton(self, focusPolicy=Qt.NoFocus)
            layout.addWidget(btn)
            btn.setFixedSize(btn_size)

            iconType = getattr(style, "SP_TitleBar{}Button".format(target.capitalize()))
            btn.setIcon(style.standardIcon(iconType))

            if target == "close":
                colorNormal = "red"
                colorHover = "orangered"
            else:
                colorNormal = "palette(mid)"
                colorHover = "palette(light)"
            btn.setStyleSheet(
                """
                QToolButton {{
                    background-color: {};
                }}
                QToolButton:hover {{
                    background-color: {}
                }}
            """.format(colorNormal, colorHover)
            )

            signal = getattr(self, target + "Clicked")
            btn.clicked.connect(signal)

            setattr(self, target + "Button", btn)

        self.normalButton.hide()

        self.updateTitle(parent.windowTitle())
        parent.windowTitleChanged.connect(self.updateTitle)

    def updateTitle(self, title=None):
        if title is None:
            title = self.window().windowTitle()
        width = self.title.width()
        width -= self.style().pixelMetric(QStyle.PM_LayoutHorizontalSpacing) * 3
        self.title.setText(self.fontMetrics().elidedText(title, Qt.ElideRight, width))

    def windowStateChanged(self, state):
        self.normalButton.setVisible(state == Qt.WindowMaximized)
        self.maxButton.setVisible(state != Qt.WindowMaximized)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.clickPos = event.windowPos().toPoint()

    def mouseMoveEvent(self, event):
        if self.clickPos is not None:
            self.window().move(event.globalPos() - self.clickPos)

    def mouseReleaseEvent(self, QMouseEvent):  # noqa: ARG002
        self.clickPos = None

    def closeClicked(self):
        self.window().close()

    def maxClicked(self):
        self.window().showMaximized()

    def normalClicked(self):
        self.window().showNormal()

    def minClicked(self):
        self.window().showMinimized()

    def resizeEvent(self, event):  # noqa: ARG002
        self.title.resize(self.width(), self.height())
        self.updateTitle()
