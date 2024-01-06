""" @file: ObjectSHA.py:
  Standardized <object id> digest to support filesystem-as-database requirements:
    * Allow verification that JSON files representing objects have not been arbitrarily moved:
      => SHA == component of object's path on filesystem;
    * Allow verification of nested objects where components may be loaded from separate JSON files:
      => SHA == SHA of parent object.
"""
from typing import Any, Optional
import hashlib
import json

from pydantic import BaseModel, Field, validator

class ObjectSHA(BaseModel):
    hex: str = Field(description='16-character lowercase hex string', min_length=16, max_length=16, pattern=r'[0-9a-f]+')
    # If we still have the decoded JSON, retain it for possible re-use (, but do not require it):
    decodedKey: Optional[str]

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
    def fromObject(obj: Any):
        # In combination with the Field validation above: this enforces the format of the SHA as a string.
        hasher = hashlib.shake_256()
        decodedKey = json.dumps(obj.__dict__).encode("utf-8")
        hasher.update(decodedKey)
        return ObjectSHA(hex=hasher.digest(8).hex(), decodedKey=decodedKey)
