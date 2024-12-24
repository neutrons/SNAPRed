from typing import Tuple

from qtpy.QtCore import Property, Slot, QPointF, Qt, QTimer
from qtpy.QtGui import QBrush, QColor, QPainter, QPen, QRadialGradient
from qtpy.QtWidgets import QAbstractButton

_styleSheet = """
    /* Color */
    qproperty-color: rgb(255, 0, 0);
    /* Intensity at edge of indicator, when on */
    qproperty-onContrast: 0.75;
    
    /* Intensity at center, when off */
    qproperty-offFactor: 0.5;
    /* Intensity at edge, when off */
    qproperty-offContrast: 0.1;
    
    /* Bezel color */
    qproperty-bezelColor: rgb(224, 224, 224);
    qproperty-bezelContrast: 0.1;
    
    /* Fixed size */
    min-width: 24px; max-width: 24px;
    min-height: 24px; max-height: 24px;
"""

class LEDIndicator(QAbstractButton):
    scaledSize = 1000.0

    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_StyleSheetTarget)
        
        self.setMinimumSize(24, 24) # 
        self.setCheckable(True)
        self.setChecked(False)

        # Default values: 
        #    will be overridden by the stylesheet.
        self._color = QColor(0, 255, 0)
        self._onContrast = 0.75
        self._offFactor = 0.5
        self._offContrast = 0.1
        self._bezelColor = QColor(224, 224, 224)
        self._bezelContrast = 0.1
        
        self._flash = False
        """ # heart beat
        self._flashColors = (QColor(0, 255, 0),)
        self._flashDurations = (0.20, 1.80)
        """
        self._flashColors = (QColor(0, 0, 255), QColor(0, 255, 0))
        self._flashDurations = (0.2, 1.80)
        
        self._flashTimer = QTimer(self)
        #   A "chain" update is used here, to prevent runaway-timer issues.
        self._flashTimer.setSingleShot(True)
        self._flashTimer.setTimerType(Qt.CoarseTimer)
        self._flashTimer.timeout.connect(self._continueFlash)
        
        
        # Override to Red using the style sheet:
        self.setStyleSheet(_styleSheet)  # THIS works!

    @Slot(QColor)
    def setColor(self, color: QColor):
        self.setFlash(False)
        self._setColor(color)
    
    def _setColor(self, color: QColor):
        self.color = color
           
    @Slot(object)
    def setFlashSequence(self, pairs: Tuple[Tuple[QColor,...], Tuple[float,...]]):
        # Set the flash sequence from QColor, duration pairs:
        #   one `QColor` => on / off flash with timing as `on: float, off: float` in seconds;
        #   otherwise the number of `QColor` and number of duration intervals must match.
        
        # Signal(Tuple[Tuple[QColor,...], Tuple[float,...]]) as Signal(object)
        
        if not self._flash:
            colors, durations = pairs
            if len(colors) == 1:
                if not len(durations) == 2:
                    raise RuntimeError(
                        "'LEDIndicator.setFlashSequence': an on off flash requires a single color, and an on-time  / off-time tuple:\n"\
                        + "  for example 'setFlashSequence(Qt.red, (1.0, 1.5))': is a one-second on, 1.5 seconds off flash sequence."
                    )                 
            elif len(colors) != len(durations):
                raise RuntimeError(
                    "'LEDIndicator.setFlashSequence': color and durations tuples must have the same length."
                )
            self._flashColors = colors
            self._flashDurations = durations
        else:
            if pairs != (self._flashColors, self._flashDurations):
                raise RuntimeError("'LEDIndicator.setFlashSequence': the flash sequence can only be changed when flash is inactive")

    @Slot()
    def _resetFlash(self):
        if self._flashTimer.isActive():
            self._flashTimer.stop()
        self.setChecked(False)
        self._flashIndex = 0
            
    @Slot(bool)
    def setFlash(self, flag: bool):
        if flag != self._flash:
            self._resetFlash()
            self._flash = flag
            if self._flash:
                self._continueFlash()
            
    @Slot()
    def _continueFlash(self):
        if self._flash:
            if self._flashIndex >= len(self._flashColors):
                self.setChecked(False) # off time
            else:
                self._setColor(self._flashColors[self._flashIndex])
                self.setChecked(True)  # on time: may be single or multi-color sequence
            ms = int(round(self._flashDurations[self._flashIndex] * 1000.0))
            self._flashIndex += 1
            self._flashIndex %= len(self._flashDurations)
            self._flashTimer.setInterval(ms)
            self._flashTimer.start()
                    
    @Property(QColor)
    def color(self) -> QColor:
        return self._color
        
    @color.setter
    def color(self, color_: QColor):
        self._color = color_
        
    @Property(float)
    def onContrast(self) -> float:
        return self._onContrast
    
    @onContrast.setter
    def onContrast(self, contrast: float):
        self._onContrast = contrast
        
    @Property(float)
    def offFactor(self) -> float:
        return self._offFactor
        
    @offFactor.setter
    def offFactor(self, factor: float):
        self._offFactor = factor
        
    @Property(float)
    def offContrast(self) -> float:
        return self._offContrast
        
    @offContrast.setter
    def offContrast(self, factor: float):
        self._offContrast = factor
        
    @Property(QColor)
    def bezelColor(self) -> QColor:
        return self._bezelColor
        
    @bezelColor.setter
    def bezelColor(self, color: QColor):
        self._bezelColor = color
    
    @Property(float)
    def bezelContrast(self) -> float:
        return self._bezelContrast
        
    @bezelContrast.setter
    def bezelContrast(self, contrast: float):
        self._bezelContrast = contrast

    @staticmethod
    def scaleColor(color: QColor, factor: float) -> QColor:
        # scale a `QColor` value by an intensity factor in `[0.0, 1.0]`.
        return QColor(
            int(round(factor * color.red())),
            int(round(factor * color.green())),
            int(round(factor * color.blue()))
        )

    """
    def changeEvent(self, event):
        print(str(event))
        if event.type() == Qt.EnabledChange:
            if not self.isEnabled():
                self._resetFlash()
    """
    def hideEvent(self, event):
        if self._flash:
            self._resetFlash()

    def showEvent(self, event):
        if self._flash:
            self._continueFlash()
                  
    def resizeEvent(self, QResizeEvent):
        self.update()

    def paintEvent(self, QPaintEvent):
        realSize = min(self.width(), self.height())

        painter = QPainter(self)
        pen = QPen(Qt.black)
        pen.setWidth(1)

        painter.setRenderHint(QPainter.Antialiasing)
        painter.translate(self.width() / 2, self.height() / 2)
        painter.scale(realSize / self.scaledSize, realSize / self.scaledSize)

        gradient = QRadialGradient(QPointF(-500, -500), 1500, QPointF(-500, -500))
        gradient.setColorAt(0, self.bezelColor)
        gradient.setColorAt(1, self.scaleColor(self.bezelColor, self.bezelContrast))
        painter.setPen(pen)
        painter.setBrush(QBrush(gradient))
        painter.drawEllipse(QPointF(0, 0), 500, 500)

        gradient = QRadialGradient(QPointF(500, 500), 1500, QPointF(500, 500))
        gradient.setColorAt(0, self.bezelColor)
        gradient.setColorAt(1, self.scaleColor(self.bezelColor, self.bezelContrast))
        painter.setPen(pen)
        painter.setBrush(QBrush(gradient))
        painter.drawEllipse(QPointF(0, 0), 450, 450)

        painter.setPen(pen)
        if self.isChecked():
            gradient = QRadialGradient(QPointF(-500, -500), 1500, QPointF(-500, -500))
            gradient.setColorAt(0, self.color)
            gradient.setColorAt(1, self.scaleColor(self.color, self.onContrast))
        else:
            gradient = QRadialGradient(QPointF(500, 500), 1500, QPointF(500, 500))
            gradient.setColorAt(0, self.scaleColor(self.color, self.offFactor))
            gradient.setColorAt(1, self.scaleColor(self.color, self.offContrast))

        painter.setBrush(gradient)
        painter.drawEllipse(QPointF(0, 0), 400, 400)
