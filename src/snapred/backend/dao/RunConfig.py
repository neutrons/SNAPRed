from typing import Optional

from pydantic import BaseModel, Field


class RunConfig(BaseModel):
    """Class to hold the instrument configuration."""

    runNumber: str = Field(description="The ID associated with the run data you wish to use")
    IPTS: Optional[str] = None
    useLiteMode: Optional[bool] = None
    maskFileName: Optional[str] = None
    maskFileDirectory: Optional[str] = None
    gsasFileDirectory: Optional[str] = None
    calibrationState: Optional[str] = None

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v
