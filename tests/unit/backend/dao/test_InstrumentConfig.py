import unittest
from typing import Any, Dict
from unittest import mock

import pytest
from util.dao import DAOFactory

from snapred.backend.dao.state.DetectorState import DetectorState


class TestInstrumentConfig(unittest.TestCase):
    TEST_SCHEMA: Dict[str, Any] = {
        "type": "object",
        "length": 16,
        "properties": {
            # PVlogs key
            "one": {
                "type": "number",
                "resolution": 0.1,
            },
            "two": {
                "type": "integer",
            },
            "three": {"type": "array", "items": {"type": "number", "resolution": 0.05}},
        },
        "required": [
            # Use the PVlogs keys here, not the "alias".
            "one",
            "two",
            "three",
        ],
        # Here we place values which are derived from other (possibly multiple) PVs, such as `deltaTheta`.
        "derivedPVs": {
            "derived_two_three": {
                # the tuple of PVs used as the key:
                #   (`list`s should be used here to help with tests, as `tuple` won't convert through JSON)
                "keyPVs": ["two", "three"],
                # a list of key-value pairs used to form the map:
                #   representation as key-value pairs prevents JSON from stringifying the keys
                "items": [[[1, [1.10, 2.20]], 1.0101], [[3, [2.25, 3.3, 4.4]], 2.0202]],
            }
        },
    }

    def test_derivedPV(self):
        TEST_SCHEMA = TestInstrumentConfig.TEST_SCHEMA
        instrumentConfig = DAOFactory.default_instrument_config.model_copy(deep=True)
        detectorState = DAOFactory.real_detector_state.model_copy(deep=True)

        assert detectorState.PVs["BL3:Mot:OpticsPos:Pos"] == 1
        assert instrumentConfig.derivedPV("deltaTheta", detectorState) == 0.0032

        detectorState.PVs["BL3:Mot:OpticsPos:Pos"] = 2
        assert instrumentConfig.derivedPV("deltaTheta", detectorState) == 0.0006667

        logs = {}
        with mock.patch.object(instrumentConfig, "stateIdSchema", TEST_SCHEMA):
            expected = TEST_SCHEMA["derivedPVs"]["derived_two_three"]["items"][0][1]
            logs = {"one": 23.456, "two": 1, "three": [1.12, 2.19]}
            detectorState = DetectorState.fromPVLogs(logs, TEST_SCHEMA)
            assert instrumentConfig.derivedPV("derived_two_three", detectorState) == expected

            expected = TEST_SCHEMA["derivedPVs"]["derived_two_three"]["items"][1][1]
            logs = {"one": 1.1004, "two": 3, "three": [2.252, 3.31, 4.38]}
            detectorState = DetectorState.fromPVLogs(logs, TEST_SCHEMA)
            assert instrumentConfig.derivedPV("derived_two_three", detectorState) == expected

            # the sub-schema for the derivedPV is not present in the schema
            logs = {"one": 1.1004, "two": 4, "three": [2.252, 3.31, 4.36]}
            detectorState = DetectorState.fromPVLogs(logs, TEST_SCHEMA)
            with pytest.raises(RuntimeError, match=".*the derived PV.*is not present in the schema.*"):
                instrumentConfig.derivedPV("not_a_derivedPV", detectorState)

            # a specific PV required to construct the composite key is not present in the logs
            logs = {"one": 1.1004, "two": 4, "three": [2.252, 3.31, 4.36]}
            # WARNING: to construct the `DetectorState` itself, all of the PVs are required.
            detectorState = DetectorState.fromPVLogs(logs, TEST_SCHEMA)
            del detectorState.PVs["two"]
            with pytest.raises(RuntimeError, match=".*the PV.*is not present in the logs.*"):
                instrumentConfig.derivedPV("derived_two_three", detectorState)

            # an entry for the composite key is not present in the schema
            logs = {"one": 1.1004, "two": 4, "three": [2.252, 3.31, 4.36]}
            detectorState = DetectorState.fromPVLogs(logs, TEST_SCHEMA)
            with pytest.raises(RuntimeError, match=".*the PVs key.*is not present in the schema.*"):
                instrumentConfig.derivedPV("derived_two_three", detectorState)

    """
    def derivedPV(self, name: str, detectorState: DetectorState) -> Any:
        # Calculate the value of a derived PV using the schema:
        #   a derived PV depends on the values of other (possibly multiple) PVs.
        if name not in self._derivedPVMap:
            raise RuntimeError(f"the derived PV '{name}' is not present in the `stateIdSchema`")
        try:
            schema = self._derivedPVMap[name]
            key = detectorState.pvLogsKey(schema["keyPVs"], self.stateIdSchema)
            value = schema["kvs"][key]

        except KeyError as e:
            raise RuntimeError(f"the PV '{e}', required to calculate '{name}' is not present in the logs")
        return value

    @model_validator(mode="after")
    def _initDerivedPVMap(self):
        # reconstruct the derived-PV map from the schema into a more convenient format
        map_ = {}
        for name, sc in self.stateIdSchema["derivedPVs"].items():
            keyPVs = sc["keyPVs"]
            kvs = [(tuple(k), v) for k, v in sc["items"]]
            map_[name] = {
                "keyPVs": keyPVs,
                "kvs": dict(kvs)
            }
        self._derivedPVMap = map_
        return self
    """
