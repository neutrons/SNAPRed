import datetime
from typing import Dict, List

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem, LiveDataArgs


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

    def diffcal_table(self, runId: str, version: int, alternativeState: str = None):
        self._tokens["workspaceType"] = "diffcal_table"
        self._tokens["runNumber"] = runId
        self._tokens["version"] = version
        self._tokens["alternativeState"] = alternativeState
        return self

    def diffcal_mask(self, runId: str, version: int, alternativeState: str = None):
        self._tokens["workspaceType"] = "diffcal_mask"
        self._tokens["runNumber"] = runId
        self._tokens["version"] = version
        self._tokens["alternativeState"] = alternativeState
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

    def liveData(self, duration: datetime.timedelta):
        self._tokens["liveDataArgs"] = LiveDataArgs(duration=duration)
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

    def build(self, reset=True) -> GroceryListItem:
        # create the grocery list item, and return it
        item = GroceryListItem(**self._tokens)
        if reset:
            self._tokens = {}
            self._hasInstrumentSource = False
        return item

    def add(self):
        self._list.append(self.build(True))

    def buildList(self) -> List[GroceryListItem]:
        if self._tokens != {}:
            self.add()
        result = self._list
        self._list = []
        return result

    def buildDict(self) -> Dict[str, GroceryListItem]:
        list_ = self.buildList()
        result = {item.propertyName: item for item in list_ if item.propertyName is not None}
        return result
