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

    def nexus(self):
        self._tokens["workspaceType"] = "nexus"
        return self

    def grouping(self):
        self._tokens["workspaceType"] = "grouping"
        return self

    def diffcal(self):
        self._tokens["workspaceType"] = "diffcal"
        return self

    def using(self, using: str):
        self._tokens["using"] = using
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
            key = list(kwarg.keys())[0]
            self._tokens["instrumentPropertySource"] = key
            self._tokens["instrumentSource"] = kwarg[key]
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
        # set either run number or grouping scheme, as needed
        if self._tokens["workspaceType"] == "grouping":
            self._tokens["groupingScheme"] = self._tokens["using"]
        else:
            self._tokens["runNumber"] = self._tokens["using"]
        del self._tokens["using"]
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
        res = {item.propertyName: item for item in self._list if item.propertyName is not None}
        self._list = []
        return res
