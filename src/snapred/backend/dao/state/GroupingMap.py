from typing import Dict, List

from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup


class GroupingMap(BaseModel):
    # allow initializtion from either dictionary or list
    focusGroupMapping: Dict[str, FocusGroup] = {}
    liteMapping: Dict[int, focusGroupMapping] = {}
    SHAs: List[str] = []
    isLite: int = 0

    @property
    def isLite(self) -> int:
        return self.isLite

    @property
    def names(self) -> List[str]:
        return [self.liteMapping[self.isLite][i].name for i in self.SHAs]

    @property
    def definitions(self) -> List[str]:
        return [self.liteMapping[self.isLite][i].definition for i in self.SHAs]

    def __getitem__(self, key):
        return self.focusGroups[key]

    def __init__(
        self,
        names: List[str] = None,
        definitions: List[str] = None,
        focusGroupMapping={},
        liteMapping={},
        SHAs=[],
        isLite=0,
    ):
        if focusGroupMapping != {}:
            liteMapping[isLite] = focusGroupMapping
        else:
            focusGroupMapping = {
                SHAs[i]: FocusGroup(
                    name=names[i],
                    definition=definitions[i],
                )
                for i in SHAs
            }
            liteMapping[isLite] = focusGroupMapping
        return super().__init__(
            focusGroupMapping=focusGroupMapping,
            liteMapping=liteMapping,
            SHAs=SHAs,
            isLite=isLite,
        )
