import enum


class StrEnum(str, enum.Enum):
    @classmethod
    def values(cls):
        return [e.value for e in cls]
