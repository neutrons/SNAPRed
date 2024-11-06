from qtpy.QtCore import Qt, Signal, Slot
from qtpy.QtGui import QFontMetrics
from qtpy.QtWidgets import QHBoxLayout, QLabel, QSlider, QWidget

# The name `IntervalSlider` is forward looking, more properly this might be called a `DurationSlider`.
# However, the expected _eventual_ usage will be as a one or two bar-button range slider.

class IntervalSlider(QWidget):
    
    def __init__(self, forwardInterval: bool=True):
        super().__init__()

        self.slider = QSlider(Qt.Horizontal)
        self._forwardInterval = forwardInterval
        
        # Style sheet specifics should now come from the application's `style.qss`.
        # self.setStyleSheet(IntervalSlider._forwardStyleSheet if self._forwardInterval else IntervalSlider._reverseStyleSheet)
        
        self.slider.setMinimum(0)
        self.slider.setMaximum(100)
        self.slider.setValue(50)
        self.slider.setTickPosition(QSlider.TicksBelow)
        self.slider.setTickInterval(10)
        self.slider.valueChanged.connect(self.on_value_changed)

        self.sliderText = QLabel()
        textMetrics = QFontMetrics(self.slider.font())
        self.sliderText.setFixedHeight(textMetrics.height() + 4)
        self.sliderText.setFixedWidth(textMetrics.width("t - 999s") + 4)
        self.sliderText.setText("50s" if self._forwardInterval else "t - 50s")
        
        layout = QHBoxLayout()
        layout.addWidget(self.slider)
        layout.addWidget(self.sliderText)
        self.setLayout(layout)
    
    @Slot(int)
    def on_value_changed(self, value):
        self.sliderText.setText(f"{value}s" if self._forwardInterval else f"t - {100 - value}s")
    
class ReverseIntervalSlider(IntervalSlider):

    def __init__(self):
        super().__init__(forwardInterval=False)


"""

/* Forward interval slider. */
QWidget#IntervalSlider::groove:horizontal {
    background: red;
    position: absolute; /* 4px from the left and right of the widget */
    left: 4px; right: 4px;
}

QWidget#IntervalSlider::handle:horizontal {
    height: 10px;
    background: green;
    margin: 0 -4px; /* expand outside the groove */
}

QWidget#IntervalSlider::add-page:horizontal {
    background: white;
}

QWidget#IntervalSlider::sub-page:horizontal {
    background: pink;
}


/* Reverse interval slider. */
IntervalSlider#ReverseIntervalSlider::groove:horizontal {
    background: red;
    position: absolute; /* 4px from the left and right of the widget */
    left: 4px; right: 4px;
}

IntervalSlider#ReverseIntervalSlider::handle:horizontal {
    height: 10px;
    background: green;
    margin: 0 -4px; /* expand outside the groove */
}

IntervalSlider#ReverseIntervalSlider::add-page:horizontal {
    background: pink;
}

IntervalSlider#ReverseIntervalSlider::sub-page:horizontal {
    background: white;
}
"""
