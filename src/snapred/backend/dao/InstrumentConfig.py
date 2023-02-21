from pydantic import BaseModel

class InstrumentConfig(BaseModel):
    """Class to hold the instrument parameters."""
    name: str
    nexusFileExtension: str
    nexusFilePrefix: str
    calibrationFileExtension: str
    calibrationFilePrefix: str
    calibrationDirectory: str
    sharedDirectory: str
    nexusDirectory: str
    reducedDataDirectory: str

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v