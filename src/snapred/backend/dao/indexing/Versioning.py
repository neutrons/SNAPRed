from typing import Any, Optional

from pydantic import BaseModel, computed_field, field_serializer
from snapred.meta.Config import Config

VERSION_START = Config["version.start"]
VERSION_NONE_NAME = Config["version.error"]
VERSION_DEFAULT_NAME = Config["version.default"]
VERSION_DEFAULT = -1  # SNAPRed Internal flag for default version


class VersionedObject(BaseModel):
    __version: Optional[int] = None

    def __init__(self, **kwargs):
        version = kwargs.pop("version", None)
        if version == VERSION_NONE_NAME:
            version = None
        elif version == VERSION_DEFAULT_NAME:
            version = VERSION_DEFAULT
        elif version == VERSION_DEFAULT or version is None:
            pass
        elif not isinstance(version, int) or version < 0:
            raise ValueError(f"Cannot initialize version as {version}")
        super().__init__(**kwargs)
        self.__version = version

    @field_serializer("version", check_fields=False, when_used="json")
    def write_user_defaults(self, value: Any):  # noqa ARG002
        if self.__version is None:
            return VERSION_NONE_NAME
        elif self.__version == VERSION_DEFAULT:
            return VERSION_DEFAULT_NAME
        else:
            return self.__version

    # NOTE some serialization still using the dict() method
    def dict(self, **kwargs):
        res = super().dict(**kwargs)
        res["version"] = self.write_user_defaults(res["version"])
        return res

    @computed_field
    @property
    def version(self) -> int:
        return self.__version

    @version.setter
    def version(self, v):
        if v == VERSION_DEFAULT_NAME:
            self.__version = VERSION_DEFAULT
        elif v == VERSION_DEFAULT:
            self.__version = v
        elif isinstance(v, int) and v >= 0:
            self.__version = v
        else:
            raise ValueError(f"Attempted to set version with invalid value {v}")
