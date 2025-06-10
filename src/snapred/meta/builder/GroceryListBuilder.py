from __future__ import annotations

import datetime
from pathlib import Path
from typing import Dict, List

from snapred.backend.dao.indexing.Versioning import Version
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem, LiveDataArgs


class GroceryListBuilder:
    def __init__(
        self,
    ):
        self._tokens = {}
        self._hasInstrumentSource = False
        self._list = []
        pass

    def neutron(self, runId: str) -> GroceryListBuilder:
        self._tokens["workspaceType"] = "neutron"
        self._tokens["runNumber"] = runId
        return self

    def grouping(self, groupingScheme: str) -> GroceryListBuilder:
        self._tokens["workspaceType"] = "grouping"
        self._tokens["groupingScheme"] = groupingScheme
        return self

    def group(self, groupingScheme: str) -> GroceryListBuilder:
        self._tokens["groupingScheme"] = groupingScheme
        return self

    def state(self, state: str) -> GroceryListBuilder:
        self._tokens["state"] = state
        return self

    def filePath(self, filePath: Path) -> GroceryListBuilder:
        self._tokens["filePath"] = filePath
        return self

    def diffCalFilePath(self, path: Path) -> GroceryListBuilder:
        self._tokens["diffCalFilePath"] = path
        return self

    def diffCalVersion(self, version: Version) -> GroceryListBuilder:
        self._tokens["diffCalVersion"] = version
        return self

    def fromRun(self, runId: str) -> GroceryListBuilder:
        self._tokens["runNumber"] = runId
        return self

    def diffcal(self, runId: str) -> GroceryListBuilder:
        self._tokens["workspaceType"] = "diffcal"
        self._tokens["runNumber"] = runId
        return self

    def diffcal_output(self, state: str, version: int) -> GroceryListBuilder:
        self._tokens["workspaceType"] = "diffcal_output"
        self._tokens["state"] = state
        self._tokens["diffCalVersion"] = version
        return self

    def diffcal_diagnostic(self, state: str, version: int) -> GroceryListBuilder:
        self._tokens["workspaceType"] = "diffcal_diagnostic"
        self._tokens["state"] = state
        self._tokens["diffCalVersion"] = version
        return self

    def diffcal_table(self, state: str, version: int, sampleRunNumber: str = None) -> GroceryListBuilder:
        self._tokens["workspaceType"] = "diffcal_table"
        self._tokens["diffCalVersion"] = version
        self._tokens["state"] = state
        # sample run number is just used as a faster intrument-donor
        if sampleRunNumber is not None:
            self._tokens["runNumber"] = sampleRunNumber
        return self

    def diffcal_mask(self, state: str, version: int, sampleRunNumber: str = None) -> GroceryListBuilder:
        self._tokens["workspaceType"] = "diffcal_mask"
        self._tokens["state"] = state
        self._tokens["diffCalVersion"] = version
        # sample run number is just used as a faster intrument-donor
        if sampleRunNumber is not None:
            self._tokens["runNumber"] = sampleRunNumber
        return self

    def normalization(self, runNumber: str, state: str, version: int) -> GroceryListBuilder:
        self._tokens["workspaceType"] = "normalization"
        self._tokens["state"] = state
        self._tokens["normCalVersion"] = version
        # NOTE: required to lookup instrument state and apply filtering
        self._tokens["runNumber"] = runNumber
        self._tokens["hidden"] = True
        return self

    def reduction_pixel_mask(self, runId: str, timestamp: float) -> GroceryListBuilder:
        self._tokens["workspaceType"] = "reduction_pixel_mask"
        self._tokens["runNumber"] = runId
        self._tokens["timestamp"] = timestamp
        return self

    def native(self) -> GroceryListBuilder:
        self._tokens["useLiteMode"] = False
        return self

    def lite(self) -> GroceryListBuilder:
        self._tokens["useLiteMode"] = True
        return self

    def useLiteMode(self, useLiteMode: bool) -> GroceryListBuilder:
        self._tokens["useLiteMode"] = useLiteMode
        return self

    def liveData(self, duration: datetime.timedelta) -> GroceryListBuilder:
        self._tokens["liveDataArgs"] = LiveDataArgs(duration=duration)
        return self

    def unit(self, unit_: str) -> GroceryListBuilder:
        self._tokens["unit"] = unit_
        return self

    def source(self, **kwarg) -> GroceryListBuilder:
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

    def name(self, name: str) -> GroceryListBuilder:
        self._tokens["propertyName"] = name
        return self

    def clean(self) -> GroceryListBuilder:
        self._tokens["keepItClean"] = True
        return self

    def dirty(self) -> GroceryListBuilder:
        self._tokens["keepItClean"] = False
        return self

    def hidden(self):
        raise NotImplementedError("hidden() is not implemented yet")

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
