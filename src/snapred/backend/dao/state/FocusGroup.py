from typing import List

from pydantic import BaseModel


class FocusGroup(BaseModel):
    name: str  # eg Column, Bank, All
    definition: str  # eg SNS/SNAP/shared/IPTS_xxx/nexus/SNAP+123.nxs
