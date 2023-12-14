from typing import Dict, List

from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup


class GroupingMap(BaseModel):
    # allow initializtion from either dictionary or list
    focusGroupMapping: Dict[str, Dict[str, str]] = {}
    liteMapping: Dict[int, focusGroupMapping] = {}
    SHAs: List[str] = []
    isLite: int = 0

    @property
    def isLite(self) -> int:
        return self.isLite

    def __init__(
        self,
        names: List[str] = None,
        definitions: List[str] = None,
        focusGroupMapping={},
        liteMapping={},
        SHAs: List[str] = None,
        isLite=0,
    ):
        if len(names) != len(definitions):
            raise ValueError("List of names and definitions required to have same length")
        if focusGroupMapping != {}:
            liteMapping[isLite] = focusGroupMapping
        else:
            if SHAs is None:
                raise ValueError("No SHAs given")
            focusGroupMapping = {SHAs[i]: {names[i]: definitions[i]} for i in SHAs}
            liteMapping[isLite] = focusGroupMapping
        return super().__init__(
            focusGroupMapping=focusGroupMapping,
            liteMapping=liteMapping,
            SHAs=SHAs,
            isLite=isLite,
        )

    def lite(self, name: str, SHA: str) -> str:
        return self.liteMapping[1][SHA][name]

    def native(self, name: str, SHA: str) -> str:
        return self.liteMapping[1][SHA][name]
