class JsonFormPresenter:
    def __init__(self, view, model) -> None:
        self.view = view
        self.model = model

    @property
    def widget(self):
        return self.view
