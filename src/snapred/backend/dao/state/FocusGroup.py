from typing import List

from pydantic import BaseModel


class FocusGroup(BaseModel):
    name: str
    definition: str
