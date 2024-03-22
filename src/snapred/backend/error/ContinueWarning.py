class ContinueWarning(Exception):
    """
    The user has chosen to do something inadvisable, but not invalid.
    Warn the user, but allow the user to continue with poor decision.
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
