from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Dict
from unittest import mock

import numpy as np


class _MockH5Dataset:
    # A simplified mock of an `h5py.Dataset` instance with attributes.
    # For its initialization and access, this assumes that the dataset contains an `Iterable` value.
    # The dataset's attributes are initialized via the `attrs: Dict[str, Any]` arg.

    def __init__(self, values: Iterable[Any], attrs: Dict[str, Any] = {}):
        self.values = values
        self.attrs = attrs

    def __getitem__(self, key):
        if key is Ellipsis:
            return self.values
        return self.values[key]


def mockH5Dataset(values, attrs={}) -> mock.Mock:
    ds = _MockH5Dataset(values, attrs)
    mock_ = mock.MagicMock(wraps=ds)
    mock_.__getitem__.side_effect = ds.__getitem__
    mock_.attrs = ds.attrs
    return mock_


class MockH5File(Mapping):
    # A simplified mock of a read-only `h5py.File` instance.
    # For its initialization and access, this assumes the following sub-structure:
    # -- a single "entry/DASlogs" group initialized from a `dict_` arg, the (assumed `Iterable`) values of which
    #    will be transferred to `_MockH5Dataset`;
    # -- a number of "entry/<key>" special values, initialized from the `kwargs`:
    #    these special-value keys include _known_ keys _only_, and are initialized with their expected types.
    # Only the `Mapping` interface, and a few special methods and properties of `h5py.File` are implemented.

    def __init__(
        self,
        # "entry/DASlogs" items:
        dict_: Dict[str, Any],
        # special values: i.e. "entry/<key>" items:
        **kwargs,
    ):
        # Create subgroup structure as self._PVLogs = {"entry": {"DASlogs": {}}}:
        self._PVLogs = dict(entry=dict(DASlogs={}))

        for k in dict_:
            self._PVLogs["entry"]["DASlogs"][k] = {"value": mockH5Dataset(dict_[k])}

        if "end_time" in kwargs:
            self._PVLogs["entry"]["end_time"] = mockH5Dataset((kwargs["end_time"].encode("utf8"),))
        if "start_time" in kwargs:
            self._PVLogs["entry"]["start_time"] = mockH5Dataset((kwargs["start_time"].encode("utf8"),))
        if "proton_charge" in kwargs:
            self._PVLogs["entry"]["proton_charge"] = mockH5Dataset(
                (kwargs["proton_charge"],), {"units": kwargs.get("charge_units", "".encode("utf8"))}
            )
        if "run_number" in kwargs:
            self._PVLogs["entry"]["run_number"] = mockH5Dataset((kwargs["run_number"].encode("utf8"),))
        if "title" in kwargs:
            self._PVLogs["entry"]["title"] = mockH5Dataset((kwargs["title"].encode("utf8"),))

        self._filename = self._randomFilename()
        self.close = mock.Mock()

    def _randomFilename(self) -> int:
        # This method allows the mock filename to be different for each `_MockLogs` instance!
        random_int64 = np.random.randint(low=np.iinfo(np.int64).min, high=np.iinfo(np.int64).max, dtype=np.int64)
        return f"mock_h5py_file_{random_int64}.h5"

    # `h5py.File.filename` property:
    @property
    def filename(self):
        return self._filename

    # ------ Pass through `Mapping` methods: ------

    def __getitem__(self, key: str) -> Any:
        value = None
        # descend into subgroups
        ks = key.split("/")
        for k in ks:
            if not bool(k):
                continue
            value = value[k] if value is not None else self._PVLogs[k]
        return value

    def __contains__(self, key: str) -> bool:
        return self._PVLogs.__contains__(key)

    def keys(self) -> Iterable:
        return self._PVLogs.keys()

    def __iter__(self) -> Iterator:
        return self._PVLogs.__iter__()

    def __len__(self) -> int:
        return self._PVLogs.__len__()


def mockH5File(dict_, **kwargs) -> mock.Mock:
    # Fully wrap the `MockH5File`.
    logs = MockH5File(dict_, **kwargs)
    mock_ = mock.MagicMock(wraps=logs)
    mock_.__getitem__.side_effect = logs.__getitem__
    mock_.__contains__.side_effect = logs.__contains__
    mock_.keys.side_effect = logs.keys
    mock_.__iter__.side_effect = logs.__iter__
    mock_.__len__.side_effect = logs.__len__
    return mock_
