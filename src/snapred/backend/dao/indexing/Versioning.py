from pydantic import BaseModel, ConfigDict, field_validator
from snapred.meta.Config import Config
from snapred.meta.Enum import StrEnum

VERSION_START = Config["version.start"]
VERSION_NONE_NAME = Config["version.friendlyName.error"]


class VersionState(StrEnum):
    DEFAULT = Config["version.friendlyName.default"]
    LATEST = "latest"
    NEXT = "next"


# I'm not sure why ci is failing without this, it doesn't seem to be used anywhere
VERSION_DEFAULT = VersionState.DEFAULT

Version = int | VersionState


class VersionedObject(BaseModel):
    # Base class for all versioned DAO

    version: Version

    @field_validator("version", mode="before")
    def validate_version(cls, value: Version) -> Version:
        if value in VersionState.values():
            return value

        if isinstance(value, str):
            raise ValueError(f"Version must be an int or {VersionState.values()}")

        if value is None:
            raise ValueError("Version must be specified")

        if value < VERSION_START:
            raise ValueError(f"Version must be greater than {VERSION_START}")

        return value

    # NOTE: This approach was taken because 'field_serializer' was checking against the
    #       INITIAL value of version for some reason.  This is a workaround.
    #
    def model_dump_json(self, *args, **kwargs):  # noqa ARG002
        if self.version in VersionState.values():
            raise ValueError(f"Version {self.version} must be flattened to an int before writing to JSON")
        return super().model_dump_json(*args, **kwargs)

    model_config = ConfigDict(use_enum_values=True, validate_assignment=True)
