class _PydanticBuilder:
    def __init__(self, orig_cls):
        self.cls = orig_cls
        self.props = {}
        # extract members variables from class
        self.members = orig_cls.model_fields.keys()

    def __getattr__(self, key):
        if key not in self.members:
            raise RuntimeError(f"Key [{key}] not a valid member for {self.cls}. Valid members are: {self.members}")

        def setValue(value):
            self.props[key] = value
            return self

        return setValue

    def build(self):
        return self.cls(**self.props)


def Builder(orig_cls):
    def builder():
        return _PydanticBuilder(orig_cls)

    orig_cls.builder = builder
    return orig_cls
