from qtpy.QtWidgets import QLabel


class DisabledFieldLabel(QLabel):
    def __init__(self, *args, **kwargs):
        super(DisabledFieldLabel, self).__init__(*args, **kwargs)
        self.setObjectName("DisabledFieldLabel")
        self.setWordWrap(True)
