import inspect
import logging

##
## In order to keep the rest of the import sequence unmodified: any test-related imports are added at the end.
##
import unittest
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Dict
from unittest import mock

import pytest
from mantid.kernel import DateAndTime
from util.h5py_helpers import mockH5File

from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.dao.state import DetectorState
from snapred.backend.data.util.PV_logs_util import *
from snapred.meta.Config import Config


class TestRunMetadata(unittest.TestCase):
    def setUp(self):
        self.DASlogsGroup = Config["instrument.PVLogs.rootGroup"]
        self.DASlogs = {
            "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
            "det_arc1": [1.25],
            "det_arc2": [2.26],
            "BL3:Det:TH:BL:Frequency": [35.0],
            "BL3:Mot:OpticsPos:Pos": [1],
            "det_lin1": [1.0],
            "det_lin2": [2.0],
        }

        """
        EXAMPLE: 'instrument.PVLogs.instrumentKeys' section from 'application.yml':
         -
           - "BL3:Chop:Skf1:WavelengthUserReq"
           - "BL3:Chop:Gbl:WavelengthReq"
           - "BL3:Det:TH:BL:Lambda"
         - "det_arc1"
         - "det_arc2"
         - "BL3:Det:TH:BL:Frequency"
         -
           - "BL3:Mot:OpticsPos:Pos"
           - "optics"
         - "det_lin1"
         - "det_lin2"
        """
        self.alternateDASlogs = {
            "BL3:Chop:Gbl:WavelengthReq": [24.0],
            "det_arc1": [1.25],
            "det_arc2": [2.26],
            "BL3:Det:TH:BL:Frequency": [35.0],
            "optics": [1],
            "det_lin1": [1.0],
            "det_lin2": [2.0],
        }

        # Special values given as kwargs:
        #   The time values need to be converted to their expected types:
        #   PVlogs return time values as `mantid.kernel.DateAndTime`, but `RunMetadata` as a DAO
        #   retains the times as `datetime.datetime`.
        self.specialValues = dict(
            # Here we use the key names from the HDF5 "entry" group:
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0,
            run_number="12345",
            title="title for 12345",
        )

        self.detectorState: DetectorState = DetectorState.fromPVLogs(self.DASlogs, DetectorState.LEGACY_SCHEMA)
        self.stateId: ObjectSHA = self.detectorState.stateId

    def tearDown(self):
        pass

    """
    Implementation notes:
      * The following `MagicMock` is setup to mock a `Mapping`.
        Due to the derived-class structure of the `RunMetadata`, for some reason this does not work
        without the additional `_MappingWrapper` implementation.
    """

    class _MappingWrapper(Mapping):
        def __init__(self, map_: Mapping):
            self._map = map_

        def __contains__(self, key) -> bool:
            return self._map.__contains__(key)

        def __getitem__(self, key) -> Any:
            return self._map.__getitem__(key)

        def __len__(self) -> int:
            return self._map.__len__()

        def __delitem__(self, key):
            self._map.__delitem__(key)

        def __iter__(self) -> Iterator:
            return self._map.__iter__()

        def keys(self) -> Iterable:
            return self._map.keys()

    @staticmethod
    def _wrapMappingMock(map_: Mapping) -> mock.Mock:
        # Wrap a `Mapping` object in a `Mock`.
        map__ = TestRunMetadata._MappingWrapper(map_)
        mock_ = mock.MagicMock(wraps=map__)
        mock_.__getitem__.side_effect = map__.__getitem__
        mock_.__contains__.side_effect = map__.__contains__
        mock_.__iter__.side_effect = map__.__iter__
        mock_.__len__.side_effect = map__.__len__
        mock_.keys.side_effect = map__.keys
        return mock_

    class _MockProperty:
        def __init__(self, value):
            self.value = value

    @staticmethod
    def _mockProperty(value) -> mock.Mock:
        return mock.Mock(wraps=TestRunMetadata._MockProperty)(value)

    class _MockRun:
        # `mantid.api.Run` implements the `PropertyManager` interface,
        #   along with additional special-value methods.

        def __init__(
            self,
            # "DASlogs" properties:
            dict_: Dict[str, Any],
            # "special value" properties:
            **kwargs,
        ):
            self._dict = dict_.copy()
            self._end_time = DateAndTime(kwargs["end_time"]) if "end_time" in kwargs else None
            self._start_time = DateAndTime(kwargs["start_time"]) if "start_time" in kwargs else None
            self._proton_charge = kwargs.get("proton_charge", None)

            # For `mantid.api.Run`: "run_number" and "run_title" go into the properties _dict:
            if "run_number" in kwargs:
                self._dict["run_number"] = kwargs["run_number"]
            if "title" in kwargs:
                self._dict["run_title"] = kwargs["title"]

        def endTime(self) -> DateAndTime:
            if self._end_time is None:
                raise RuntimeError("no end time has been set for this run")
            return self._end_time

        def startTime(self) -> DateAndTime:
            if self._start_time is None:
                raise RuntimeError("no start time has been set for this run")
            return self._start_time

        def getProtonCharge(self) -> float:
            if self._proton_charge is None:
                raise RuntimeError("proton charge log is empty")
            return self._proton_charge

        def hasProperty(self, key: str) -> bool:
            return key in self._dict

        def getProperty(self, key: str) -> "TestMappingFromRun._MockProperty":
            if key not in self._dict:
                raise RuntimeError(f"Unknown property search object {key}")
            return TestRunMetadata._mockProperty(self._dict[key])

        def keys(self):
            return self._dict.keys()

    @staticmethod
    def _mockRun(dict_, **kwargs) -> mock.Mock:
        run = TestRunMetadata._MockRun(dict_, **kwargs)
        return mock.Mock(wraps=run)

    def test_init_fromRun(self):
        RunMetadata.fromRun(self._mockRun(self.DASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA)

    def test_init_fromNeXusLogs(self):
        logs = mockH5File(self.DASlogs, **self.specialValues)
        RunMetadata.fromNeXusLogs(logs, DetectorState.LEGACY_SCHEMA)

    def _test_get_item(self, map_: RunMetadata):
        # Verify that special values have been initialized:
        assert map_.runNumber == self.specialValues["run_number"]
        assert map_.runTitle == self.specialValues["title"]
        assert map_.startTime == datetime.datetime.replace(
            datetime.datetime.fromisoformat(self.specialValues["start_time"]), tzinfo=datetime.timezone.utc
        )
        assert map_.endTime == datetime.datetime.replace(
            datetime.datetime.fromisoformat(self.specialValues["end_time"]), tzinfo=datetime.timezone.utc
        )
        assert map_.protonCharge == self.specialValues["proton_charge"]
        assert map_.detectorState == self.detectorState
        assert map_.stateId == self.stateId

        # Verify all other keys:
        for k in self.DASlogs:
            assert map_[k] == self.DASlogs[k]

    def test_get_item_fromRun(self):
        map_ = RunMetadata.fromRun(self._mockRun(self.DASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA)
        self._test_get_item(map_)

    def test_get_item_fromNeXusLogs(self):
        map_ = RunMetadata.fromNeXusLogs(mockH5File(self.DASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA)
        self._test_get_item(map_)

    def _test_iter(self, map_):
        expectedKeys = set(self.DASlogs.keys())
        expectedKeys.update(("run_number", "run_title"))
        for k in map_:
            assert k in expectedKeys

    def test_iter_fromRun(self):
        map_ = RunMetadata.fromRun(self._mockRun(self.DASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA)
        self._test_iter(map_)

    def test_iter_fromNeXusLogs(self):
        map_ = RunMetadata.fromNeXusLogs(mockH5File(self.DASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA)
        self._test_iter(map_)

    def _test_len(self, map_):
        expectedKeys = set(self.DASlogs.keys())
        expectedKeys.update(("run_number", "run_title"))
        assert len(map_) == len(expectedKeys)

    def test_len_fromRun(self):
        map_ = RunMetadata.fromRun(self._mockRun(self.DASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA)
        self._test_len(map_)

    def test_len_fromNeXusLogs(self):
        map_ = RunMetadata.fromNeXusLogs(mockH5File(self.DASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA)
        self._test_len(map_)

    def test_proton_charge_units_default_fromNeXusLogs(self):
        logs = mockH5File(self.DASlogs, **self.specialValues)

        map_ = RunMetadata.fromNeXusLogs(logs, DetectorState.LEGACY_SCHEMA)

        # Default charge units are "uAh" => no conversion is required
        assert pytest.approx(map_.protonCharge, 1.0e-9) == logs["entry"]["proton_charge"][0]

    def test_proton_charge_units_uAh_fromNeXusLogs(self):
        logs = mockH5File(self.DASlogs, **dict(**self.specialValues, charge_units="uAh".encode("utf8")))

        map_ = RunMetadata.fromNeXusLogs(logs, DetectorState.LEGACY_SCHEMA)
        # Default charge units are "uAh" => no conversion is required
        assert pytest.approx(map_.protonCharge, 1.0e-9) == logs["entry"]["proton_charge"][0]

    def test_proton_charge_units_picoCoulomb_fromNeXusLogs(self):
        logs = mockH5File(self.DASlogs, **dict(**self.specialValues, charge_units="picoCoulomb".encode("utf8")))

        map_ = RunMetadata.fromNeXusLogs(logs, DetectorState.LEGACY_SCHEMA)

        # Conversion factor between picoColumbs and microAmp*hours
        chargeFactor = 1.0e-6 / 3600.0
        assert pytest.approx(map_.protonCharge, 1.0e-9) == chargeFactor * logs["entry"]["proton_charge"][0]

    def _test_contains_primary(self, map_):
        # Test that any '__contains__' with a primary key references the primary-key only.
        """
        EXAMPLE: 'instrument.PVLogs.instrumentKeys' section from 'application.yml':
         -
           - "BL3:Chop:Skf1:WavelengthUserReq"
           - "BL3:Chop:Gbl:WavelengthReq"
           - "BL3:Det:TH:BL:Lambda"
         - "det_arc1"
         - "det_arc2"
         - "BL3:Det:TH:BL:Frequency"
         -
           - "BL3:Mot:OpticsPos:Pos"
           - "optics"
         - "det_lin1"
         - "det_lin2"
        """
        logs = self.DASlogs
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                # make sure keys have been initialized for this test
                assert ks in logs
                # make sure all keys are in the mapping
                assert ks in map_
            else:
                # make sure that only the primary key is referencable
                assert ks[0] in logs
                assert ks[0] in map_
                for k in ks[1:]:
                    assert k not in map_

    def test_contains_primary_fromRun(self):
        map_ = RunMetadata.fromRun(self._mockRun(self.DASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA)
        self._test_contains_primary(map_)

    def test_contains_primary_fromNeXusLogs(self):
        map_ = RunMetadata.fromNeXusLogs(mockH5File(self.DASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA)
        self._test_contains_primary(map_)

    def _test_contains_alternatives(self, map_):
        # Test that any '__contains__' with a primary key references the alternative-key.
        """
        EXAMPLE: 'instrument.PVLogs.instrumentKeys' section from 'application.yml':
         -
           - "BL3:Chop:Skf1:WavelengthUserReq"
           - "BL3:Chop:Gbl:WavelengthReq"
           - "BL3:Det:TH:BL:Lambda"
         - "det_arc1"
         - "det_arc2"
         - "BL3:Det:TH:BL:Frequency"
         -
           - "BL3:Mot:OpticsPos:Pos"
           - "optics"
         - "det_lin1"
         - "det_lin2"
        """
        logs = self.alternateDASlogs

        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                # make sure keys have been initialized for this test
                assert ks in logs
                # make sure all keys are in the mapping
                assert ks in map_
            else:
                # primary key references alternative key
                assert ks[0] not in logs
                assert ks[0] in map_
                # alternative key continues to reference itself
                assert ks[1] in map_

    def test_contains_alternatives_fromRun(self):
        map_ = RunMetadata.fromRun(
            self._mockRun(self.alternateDASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA
        )
        self._test_contains_alternatives(map_)

    def test_contains_alternatives_fromNeXusLogs(self):
        map_ = RunMetadata.fromNeXusLogs(
            mockH5File(self.alternateDASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA
        )
        self._test_contains_alternatives(map_)

    def _test_keys_alternatives(self, map_):
        # Test that both primary keys and any existing altarnative-keys show up in the 'keys()' return value.
        """
        EXAMPLE: 'instrument.PVLogs.instrumentKeys' section from 'application.yml':
         -
           - "BL3:Chop:Skf1:WavelengthUserReq"
           - "BL3:Chop:Gbl:WavelengthReq"
           - "BL3:Det:TH:BL:Lambda"
         - "det_arc1"
         - "det_arc2"
         - "BL3:Det:TH:BL:Frequency"
         -
           - "BL3:Mot:OpticsPos:Pos"
           - "optics"
         - "det_lin1"
         - "det_lin2"
        """
        logs = self.alternateDASlogs
        keys_ = map_.keys()
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                # make sure keys have been initialized for this test
                assert ks in logs
                # make sure all keys are in the mapping
                assert ks in keys_
            else:
                # primary key references alternative key
                assert ks[0] not in logs
                assert ks[0] in keys_
                # alternative key continues to reference itself
                assert ks[1] in keys_

    def test_keys_alternatives_fromRun(self):
        map_ = RunMetadata.fromRun(
            self._mockRun(self.alternateDASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA
        )
        self._test_keys_alternatives(map_)

    def test_keys_alternatives_fromNeXusLogs(self):
        map_ = RunMetadata.fromNeXusLogs(
            mockH5File(self.alternateDASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA
        )
        self._test_keys_alternatives(map_)

    def _test_get_alternatives(self, map_):
        # Test that any 'get' with a primary key references the altarnative-key value.
        """
        EXAMPLE: 'instrument.PVLogs.instrumentKeys' section from 'application.yml':
         -
           - "BL3:Chop:Skf1:WavelengthUserReq"
           - "BL3:Chop:Gbl:WavelengthReq"
           - "BL3:Det:TH:BL:Lambda"
         - "det_arc1"
         - "det_arc2"
         - "BL3:Det:TH:BL:Frequency"
         -
           - "BL3:Mot:OpticsPos:Pos"
           - "optics"
         - "det_lin1"
         - "det_lin2"
        """
        logs = self.alternateDASlogs
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                assert map_[ks] == logs[ks]
            else:
                # primary key references alternative key
                assert map_[ks[0]] == logs[ks[1]]
                # alternative key continues to reference itself
                assert map_[ks[1]] == logs[ks[1]]

    def test_get_alternatives_fromRun(self):
        map_ = RunMetadata.fromRun(
            self._mockRun(self.alternateDASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA
        )
        self._test_get_alternatives(map_)

    def test_get_alternatives_fromNeXusLogs(self):
        map_ = RunMetadata.fromNeXusLogs(
            mockH5File(self.alternateDASlogs, **self.specialValues), DetectorState.LEGACY_SCHEMA
        )
        self._test_get_alternatives(map_)

    def _test_default_start_and_end_times(self, mapFunc, mockLogs):
        # test that default start and end times are "now"
        # -- `mapFunc`: either `RunMetadata.fromRun` or `RunMetadata.fromNeXusLogs`;
        # -- `mockLogs`: either `self._mockRun` or `mockH5File`.
        now_ = datetime.datetime.utcnow()
        with mock.patch.object(inspect.getmodule(RunMetadata), "datetime") as mockDateTime:
            mockDateTime.datetime.utcnow.return_value = now_
            map_ = mapFunc(
                mockLogs(
                    self.DASlogs,
                    # no "startTime" or "endTime"
                    proton_charge=1000.0,
                    run_number=self.specialValues["run_number"],
                    title=self.specialValues["title"],
                ),
                DetectorState.LEGACY_SCHEMA,
            )
            assert map_.startTime == now_
            assert map_.endTime == now_

    def test_default_start_and_end_times_fromRun(self):
        self._test_default_start_and_end_times(RunMetadata.fromRun, self._mockRun)

    def test_default_start_and_end_times_fromNeXusLogs(self):
        self._test_default_start_and_end_times(RunMetadata.fromNeXusLogs, mockH5File)

    def _test_default_proton_charge(self, mapFunc, mockLogs):
        # test that default proton charge is zero
        # -- `mapFunc`: either `RunMetadata.fromRun` or `RunMetadata.fromNeXusLogs`;
        # -- `mockLogs`: either `self._mockRun` or `mockH5File`.

        map_ = mapFunc(
            mockLogs(
                self.DASlogs,
                # no "protonCharge"
                start_time="2010-01-01T00:00:00",
                end_time="2010-01-01T01:00:00",
                run_number=self.specialValues["run_number"],
                run_title=self.specialValues["title"],
            ),
            DetectorState.LEGACY_SCHEMA,
        )
        assert map_.protonCharge == 0.0

    def test_default_proton_charge_fromRun(self):
        self._test_default_proton_charge(RunMetadata.fromRun, self._mockRun)

    def test_default_proton_charge_fromNeXusLogs(self):
        self._test_default_proton_charge(RunMetadata.fromNeXusLogs, mockH5File)

    def _test_default_run_number(self, mapFunc, mockLogs):
        # test that default 'runNumber' is int(0)
        # -- `mapFunc`: either `RunMetadata.fromRun` or `RunMetadata.fromNeXusLogs`;
        # -- `mockLogs`: either `self._mockRun` or `mockH5File`.

        map_ = mapFunc(
            mockLogs(
                self.DASlogs,
                # no "runNumber"
                start_time="2010-01-01T00:00:00",
                end_time="2010-01-01T01:00:00",
                proton_charge=1000.0,
                run_title=self.specialValues["title"],
            ),
            DetectorState.LEGACY_SCHEMA,
        )
        assert map_.runNumber == 0

    def test_default_run_number_fromRun(self):
        self._test_default_run_number(RunMetadata.fromRun, self._mockRun)

    def test_default_run_number_fromNeXusLogs(self):
        # The 'entry/run_number' dataset should always be present in the NeXus-format HDF5 file.
        with pytest.raises(RuntimeError, match=".*the 'run_number' dataset must be added.*"):
            self._test_default_run_number(RunMetadata.fromNeXusLogs, mockH5File)

    def _test_defaults_log_warning(self, mapFunc, mockLogs, neXusFormat: bool):
        # Test that any default-value substitution logs messages at debug logging level.
        # -- `mapFunc`: either `RunMetadata.fromRun` or `RunMetadata.fromNeXusLogs`;
        # -- `mockLogs`: either `self._mockRun` or `mockH5File`.

        defaultKeys = ("runNumber", "runTitle", "startTime", "endTime", "protonCharge")

        # Note that the "entry/run_number" dataset must always be present in a NeXus-format HDF5 file.
        logs = mockLogs(self.DASlogs, **({} if not neXusFormat else {"run_number": "12345"}))
        with mock.patch.object(inspect.getmodule(RunMetadata), "logger") as mockLogger:
            map_ = mapFunc(logs, DetectorState.LEGACY_SCHEMA)  # noqa: F841
            assert mockLogger.isEnabledFor.call_count == 2
            mockLogger.isEnabledFor.assert_any_call(logging.DEBUG)
            assert mockLogger.debug.call_count == len(defaultKeys)
            for k in defaultKeys:
                callFound = True
                for call in mockLogger.debug.mock_calls:
                    if k in call.args[0]:
                        callFound = True
                        break
                assert callFound, f"no default warning logged for kwarg '{k}'"

    def test_defaults_log_warning_fromRun(self):
        self._test_defaults_log_warning(RunMetadata.fromRun, self._mockRun, False)

    def test_defaults_log_warning_fromNeXusLogs(self):
        self._test_defaults_log_warning(RunMetadata.fromNeXusLogs, mockH5File, True)
