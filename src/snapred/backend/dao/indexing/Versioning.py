from typing import Any, Optional

from numpy import integer
from pydantic import BaseModel, computed_field, field_serializer
from snapred.meta.Config import Config

VERSION_START = Config["version.start"]
VERSION_NONE_NAME = Config["version.error"]
VERSION_DEFAULT_NAME = Config["version.default"]
VERSION_DEFAULT = -1  # SNAPRed Internal flag for default version


class VersionedObject(BaseModel):
    __version: Optional[int] = None

    @classmethod
    def parseVersion(cls, version, *, exclude_none: bool = False) -> int | None:
        v: int | None
        # handle two special cases
        if (not exclude_none) and (version is None or version == VERSION_NONE_NAME):
            v = None
        elif version == VERSION_DEFAULT_NAME or version == VERSION_DEFAULT:
            v = VERSION_DEFAULT
        # parse integers
        elif isinstance(version, int | integer):
            if int(version) >= VERSION_START:
                v = int(version)
            else:
                raise ValueError(f"Given version {version} is smaller than start version {VERSION_START}")
        # otherwise this is an error
        else:
            raise ValueError(f"Cannot initialize version as {version}")
        return v

    @classmethod
    def writeVersion(cls, version) -> int | str:
        v: int | str
        if version is None:
            v = VERSION_NONE_NAME
        elif version == VERSION_DEFAULT:
            v = VERSION_DEFAULT_NAME
        elif isinstance(version, int | integer):
            v = int(version)
        else:
            raise ValueError("Version is not valid")
        return v

    def __init__(self, **kwargs):
        version = kwargs.pop("version", None)
        super().__init__(**kwargs)
        self.__version = self.parseVersion(version)

    @field_serializer("version", check_fields=False, when_used="json")
    def write_user_defaults(self, value: Any):  # noqa ARG002
        return self.writeVersion(self.__version)

    # NOTE some serialization still using the dict() method
    def dict(self, **kwargs):
        res = super().dict(**kwargs)
        res["version"] = self.writeVersion(res["version"])
        return res

    @computed_field
    @property
    def version(self) -> int:
        return self.__version

    @version.setter
    def version(self, v):
        self.__version = self.parseVersion(v, exclude_none=True)
