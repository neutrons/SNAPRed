from typing import Dict, List

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem


class GroceryListBuilder:
    def __init__(
        self,
    ):
        self._tokens = {}
        self._hasInstrumentSource = False
        self._list = []
        pass

    def neutron(self, runId: str):
        self._tokens["workspaceType"] = "neutron"
        self._tokens["runNumber"] = runId
        return self

    def loader(self, loader: str, loaderArgs: str = ""):
        self._tokens["loader"] = loader
        self._tokens["loaderArgs"] = loaderArgs
    
    def grouping(self, groupingScheme: str):
        self._tokens["workspaceType"] = "grouping"
        self._tokens["groupingScheme"] = groupingScheme
        return self

    def group(self, groupingScheme: str):
        self._tokens["groupingScheme"] = groupingScheme
        return self

    def fromRun(self, runId: str):
        self._tokens["runNumber"] = runId
        return self

    def diffcal(self, runId: str):
        self._tokens["workspaceType"] = "diffcal"
        self._tokens["runNumber"] = runId
        return self

    def diffcal_output(self, runId: str, version: int):
        self._tokens["workspaceType"] = "diffcal_output"
        self._tokens["runNumber"] = runId
        self._tokens["version"] = version
        return self

    def diffcal_diagnostic(self, runId: str, version: int):
        self._tokens["workspaceType"] = "diffcal_diagnostic"
        self._tokens["runNumber"] = runId
        self._tokens["version"] = version
        return self

    def diffcal_table(self, runId: str, version: int):
        self._tokens["workspaceType"] = "diffcal_table"
        self._tokens["runNumber"] = runId
        self._tokens["version"] = version
        return self

    def diffcal_mask(self, runId: str, version: int):
        self._tokens["workspaceType"] = "diffcal_mask"
        self._tokens["runNumber"] = runId
        self._tokens["version"] = version
        return self

    def normalization(self, runId: str, version: int):
        self._tokens["workspaceType"] = "normalization"
        self._tokens["runNumber"] = runId
        self._tokens["version"] = version
        return self

    def reduction_pixel_mask(self, runId: str, timestamp: float):
        self._tokens["workspaceType"] = "reduction_pixel_mask"
        self._tokens["runNumber"] = runId
        self._tokens["timestamp"] = timestamp
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

    def unit(self, unit_: str):
        self._tokens["unit"] = unit_
        return self

    def source(self, **kwarg):
        # This setter is retained primarily to allow overriding the
        #   automatic instrument-donor caching system.
        if len(kwarg.keys()) > 1:
            raise RuntimeError("You can only specify one instrument source")
        else:
            self._hasInstrumentSource = True
            instrumentPropertySource, instrumentSource = list(kwarg.items())[0]
            self._tokens["instrumentPropertySource"] = instrumentPropertySource
            self._tokens["instrumentSource"] = instrumentSource
        return self

    def name(self, name: str):
        self._tokens["propertyName"] = name
        return self

    def clean(self):
        self._tokens["keepItClean"] = True
        return self

    def dirty(self):
        self._tokens["keepItClean"] = False
        return self

    def build(self) -> GroceryListItem:
        # create the grocery item list, and return
        return GroceryListItem(**self._tokens)

    def add(self):
        self._list.append(self.build())
        self._tokens = {}
        self._hasInstrumentSource = False

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
