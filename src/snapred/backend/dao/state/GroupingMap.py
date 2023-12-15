from typing import Dict, List

from pydantic import BaseModel

from snapred.backend.dao.state.FocusGroup import FocusGroup


class GroupingMap(BaseModel):
    focusGroupMapping: Dict[str, Dict[str, FocusGroup]] = {}
    SHA: str = None

    @property
    def lite(self) -> Dict[str, FocusGroup]:
        return self.focusGroupMapping["lite"]

    @property
    def native(self) -> Dict[str, FocusGroup]:
        return self.focusGroupMapping["native"]

    def __init__(
        self,
        liteFocusGroups: List[FocusGroup] = None,
        nativeFocusGroups: List[FocusGroup] = None,
        focusGroupMapping={},
        SHA: str = None,
    ):
        focusGroupMapping = {
            "native": {nfg.name: nfg for nfg in nativeFocusGroups},
            "lite": {lfg.name: lfg for lfg in liteFocusGroups},
        }
        return super().__init__(
            focusGroupMapping=focusGroupMapping,
            SHA=SHA,
        )
