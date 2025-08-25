import hashlib
import json
from typing import Any, Optional

from pydantic import BaseModel, Field


class ObjectSHA(BaseModel):
    """

    Provides a standardized object identifier digest to support filesystem-as-database requirements.

    This class allows for:

    - Verification that JSON files representing objects have not been arbitrarily moved.
      This is ensured by making the SHA a component of the object's path on the filesystem.
    - Verification of nested objects where components may be loaded from separate JSON files.
      This is achieved by marking both nested object and its parent with the SHA from some common reference object,
      for example, a `DetectorState`.

    """

    hex: str = Field(description="16-character lowercase hex string", min_length=16, pattern=r"[0-9a-f]+")
    # If we still have the decoded JSON, retain it for possible re-use (, but do not require it):
    decodedKey: Optional[str] = None

    def __eq__(self, other):
        if isinstance(other, str):
            return self.hex == other
        if isinstance(other, self.__class__):
            # This requires either both must have all fields or neither has a 'decodedKey'.
            return self.__dict__ == other.__dict__
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return self.hex

    @staticmethod
    def fromObject(obj: Any, length=16):
        # In combination with the Field validation above:
        #   this method enforces the format of the SHA as a string: it must have at least 16 digits.
        dict_ = obj if isinstance(obj, dict) else obj.model_dump() if isinstance(obj, BaseModel) else obj.__dict__

        hasher = hashlib.shake_256()
        decodedKey = json.dumps(dict_).encode("utf-8")
        hasher.update(decodedKey)
        requiredBytes = length // 2 if not bool(length % 2) else length // 2 + 1
        return ObjectSHA(hex=hasher.digest(requiredBytes).hex()[:length], decodedKey=decodedKey)
