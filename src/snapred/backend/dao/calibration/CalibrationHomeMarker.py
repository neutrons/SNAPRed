from typing import ClassVar

from pydantic import BaseModel, Field, field_serializer, field_validator

from snapred.meta.Config import DeployEnvEnum
from snapred.meta.Time import isoFromTimestamp, parseTimestamp, timestamp


class CalibrationHomeMarker(BaseModel, extra="forbid"):
    """
    Marker file at the root of a deployed calibration home.

    It records the calibration folder's schema version, and the deploy environment the folder is intended for,
    so that deployed software can refuse to use (or write to) the wrong calibration folder.
    """

    # deliberately not a dotfile, so that it is easy to find
    FILENAME: ClassVar[str] = "calibrationHome.json"

    schemaVersion: int
    environment: DeployEnvEnum
    createdBy: str
    createdAt: float = Field(default_factory=timestamp)

    @field_validator("createdAt", mode="before")
    @classmethod
    def validate_createdAt(cls, v):
        return parseTimestamp(v)

    @field_serializer("createdAt")
    @classmethod
    def serialize_createdAt(cls, v):
        return isoFromTimestamp(v)
