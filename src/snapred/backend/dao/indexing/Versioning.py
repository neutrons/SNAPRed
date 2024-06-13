from typing import Literal, Union

from snapred.meta.Config import Config

UNINITIALIZED = Config["version.error"]
VERSION_DEFAULT = Config["version.default"]
VERSION_START = Config["version.start"]
Version = Union[int, Literal[UNINITIALIZED, VERSION_DEFAULT]]
