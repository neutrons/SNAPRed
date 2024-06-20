from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, computed_field, field_serializer
from snapred.meta.Config import Config

VERSION_START = Config["version.start"]
VERSION_NONE = Config["version.error"]
VERSION_DEFAULT_NAME = Config["version.default"]
VERSION_DEFAULT = -1  # SNAPRed Internal flag for default version

Version = Union[int, Literal[VERSION_NONE, VERSION_DEFAULT_NAME]]


class VersionedObject(BaseModel):
    _version: Optional[int] = None

    def __init__(self, **kwargs):
        version = kwargs.pop("version", None)
        if version == VERSION_NONE:
            version = None
        elif version == VERSION_DEFAULT_NAME:
            version = VERSION_DEFAULT
        else:
            version
        super().__init__(**kwargs)
        self._version = version

    @field_serializer("version", check_fields=False, when_used="json")
    def write_user_defaults(self, value: Any):  # noqa ARG002
        if self._version is None:
            return VERSION_NONE
        elif self._version == VERSION_DEFAULT:
            return VERSION_DEFAULT_NAME
        else:
            return self._version

    @computed_field
    @property
    def version(self) -> int:
        return self._version

    @version.setter
    def version(self, v):
        self._version = None
        if v == VERSION_DEFAULT_NAME:
            self._version = VERSION_DEFAULT
        elif v == VERSION_DEFAULT:
            self._version = v
        elif isinstance(v, int) and v >= 0:
            self._version = v
        else:
            raise ValueError(f"Attempted to set version with invalid value {v}")
