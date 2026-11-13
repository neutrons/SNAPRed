from snapred.meta.Enum import StrEnum


class Resolution(StrEnum):
    """The pixel resolution of instrument data."""

    NATIVE = "native"
    LITE = "lite"

    @classmethod
    def fromLiteMode(cls, useLiteMode: bool) -> "Resolution":
        return cls.LITE if useLiteMode else cls.NATIVE

    @property
    def useLiteMode(self) -> bool:
        return self is Resolution.LITE
