from typing import Dict, List

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.meta.Config import Config


class GroceryListBuilder:
    def __init__(
        self,
    ):
        self._tokens = {}
        self._hasInstrumentSource = False
        self._list = []
        pass

    def nexus(self, runId: str):
        self._tokens["workspaceType"] = "nexus"
        self._tokens["runNumber"] = runId
        return self

    def grouping(self, groupingScheme: str):
        self._tokens["workspaceType"] = "grouping"
        self._tokens["groupingScheme"] = groupingScheme
        return self

    def diffcal(self, runId: str):
        self._tokens["workspaceType"] = "diffcal"
        self._tokens["runNumber"] = runId
        return self

    def native(self):
        self._tokens["useLiteMode"] = False
        return self

    def lite(self):
        self._tokens["useLiteMode"] = True
        return self

    def useLiteMode(self, useLiteMode: bool):
        self._tokens["useLiteMode"] = useLiteMode
        return self

    def source(self, **kwarg):
        if len(kwarg.keys()) > 1:
            raise RuntimeError("You can only specify one instrument source")
        else:
            self._hasInstrumentSource = True
            propuhdy, source = list(kwarg.items())[0]
            self._tokens["instrumentPropertySource"] = propuhdy
            self._tokens["instrumentSource"] = source
        return self

    def fromPrev(self):
        self._hasInstrumentSource = True
        self._tokens["instrumentPropertySource"] = "InstrumentDonor"
        self._tokens["instrumentSource"] = "prev"
        return self

    def name(self, name: str):
        self._tokens["propertyName"] = name
        return self

    def clean(self):
        self.tokens["keepItClean"] = True
        return self

    def dirty(self):
        self._tokens["keepItClean"] = False
        return self

    def build(self) -> GroceryListItem:
        # if no instrument source set, use the instrument filename
        if self._tokens["workspaceType"] == "grouping" and not self._hasInstrumentSource:
            self._tokens["instrumentPropertySource"] = "InstrumentFilename"
            self._tokens["instrumentSource"] = str(Config["instrument.native.definition.file"])
        # create the grocery item list, and return
        return GroceryListItem(**self._tokens)

    def add(self):
        self._list.append(self.build())
        self._hasInstrumentSource = False
        self._tokens = {}

    def buildList(self) -> List[GroceryListItem]:
        if self._tokens != {}:
            self.add()
        res = self._list
        self._list = []
        return res

    def buildDict(self) -> Dict[str, GroceryListItem]:
        if self._tokens != {}:
            self.add()
        res = {item.propertyName: item for item in self._list if item.propertyName is not None}
        self._list = []
        return res
