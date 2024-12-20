import enum


class StrEnum(str, enum.Enum):
    def __str__(self):
        # Match behavior of Python `StrEnum` (>= 3.11)
        return self.value

    @classmethod
    def values(cls):
        return [e.value for e in cls]
