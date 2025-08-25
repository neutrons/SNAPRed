import json
from collections.abc import Mapping
from itertools import permutations
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict
from unittest import mock

import numpy as np
import pytest

from snapred.backend.dao.state import DetectorState
from snapred.meta.Config import Resource


class TestDetectorState:
    @pytest.fixture(autouse=True)
    def setUp(self):
        # setup
        self.runInfo = {
            "59039": {
                "hash": "04bd2c53f6bf6754",
                "detectorState": {
                    "arc": [-65.3, 104.95],
                    "wav": 2.1,
                    "freq": 59.999996185302734,
                    "guideStat": 1,
                    "lin": [0.045, 0.043],
                },
            },
            "61991": {
                "hash": "ffefaa93ccb23678",
                "detectorState": {
                    "arc": [-65.3, 104.95],
                    "wav": 2.1,
                    "freq": 60.0,
                    "guideStat": 2,
                    "lin": [0.045, 0.043],
                },
            },
            "64858": {
                "hash": "861d38b2a12abcfa",
                "detectorState": {
                    "arc": [-50.02, 89.96],
                    "wav": 2.1,
                    "freq": 60.0,
                    "guideStat": 2,
                    "lin": [0.045, 0.043],
                },
            },
        }

        return
        # teardown follows

    def _logs_from_DetectorState(self, detectorState: Dict[str, Any]) -> Mapping[str, Any]:
        # PV-log keys from legacy `DetectorState` field names
        return {
            "det_arc1": detectorState["arc"][0],
            "det_arc2": detectorState["arc"][1],
            "BL3:Chop:Skf1:WavelengthUserReq": detectorState["wav"],
            "BL3:Det:TH:BL:Frequency": detectorState["freq"],
            "BL3:Mot:OpticsPos:Pos": detectorState["guideStat"],
            "det_lin1": detectorState["lin"][0],
            "det_lin2": detectorState["lin"][1],
        }

    def test_legacy_hashcodes(self):
        for run, info in self.runInfo.items():
            expectedHash = info["hash"]
            PVs = self._logs_from_DetectorState(info["detectorState"])
            assert DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA).stateId.hex == expectedHash

    def test_legacy_init(self):
        for run, info in self.runInfo.items():
            expectedHash = info["hash"]
            dict_ = info["detectorState"]
            detectorState = DetectorState(
                arc=dict_["arc"], wav=dict_["wav"], freq=dict_["freq"], guideStat=dict_["guideStat"], lin=dict_["lin"]
            )
            assert detectorState.stateId.hex == expectedHash

    def test_legacy_init_exceptions(self):
        run, info = next(iter(self.runInfo.items()))
        dict_ = info["detectorState"]
        with pytest.raises(ValueError, match=".*field.* is missing from legacy `DetectorState`.*"):
            DetectorState(arc=dict_["arc"], freq=dict_["freq"], guideStat=dict_["guideStat"], lin=dict_["lin"])

    def test_fromPVLogs_normalizes_type(self):
        # verify that `fromPVLogs` factory method applies type normalization to the PVs
        #   stored in `DetectorState.PVs`. verify that the method does NOT round the stored PV values.
        PVs_schema = DetectorState.LEGACY_SCHEMA["properties"]
        run, info = next(iter(self.runInfo.items()))
        PVs = self._logs_from_DetectorState(info["detectorState"])

        # Type normalization is applied to the stored PV values.
        type_normalized_PVs = {
            k: DetectorState._normalize_type(v, type_=PVs_schema[k]["type"], name=k) for k, v in PVs.items()
        }

        # Rounding-to-resolution is additionally applied to stored PV values as used in the hash computation.
        rounded_PVs = {
            k: DetectorState._round_to_resolution(v, resolution=PVs_schema[k].get("resolution", 0.0))
            for k, v in type_normalized_PVs.items()
        }

        # These assertions happen to be valid for the `LEGACY_SCHEMA`,
        #   which is useful for this test, but in for other schemas is not necessarily the case.
        assert PVs != type_normalized_PVs
        assert type_normalized_PVs != rounded_PVs

        with (
            mock.patch.object(
                DetectorState, "_normalize_type", new=mock.Mock(side_effect=DetectorState._normalize_type)
            ) as mock__normalize_type,
            mock.patch.object(
                DetectorState, "_round_to_resolution", new=mock.Mock(side_effect=DetectorState._round_to_resolution)
            ) as mock__round_to_resolution,
        ):
            detectorState = DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA)

            assert mock__normalize_type.call_count == len(PVs)

            # `_round_to_resolution` _is_ called for the SHA computation
            mock__round_to_resolution.call_count == len(PVs)

            assert detectorState.PVs == type_normalized_PVs

    def test_fromPVLogs_exceptions(self):
        run, info = next(iter(self.runInfo.items()))
        PVs = self._logs_from_DetectorState(info["detectorState"])
        del PVs["det_arc2"]
        with pytest.raises(RuntimeError, match=".*a required state PV.*is not present.*"):
            DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA)

    def test_toPVLogs(self):
        # verify the `toPVLogs` converts to "time series" (as `Iterable`)
        run, info = next(iter(self.runInfo.items()))
        PVs = self._logs_from_DetectorState(info["detectorState"])
        detectorState = DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA)
        PVs_ = detectorState.toPVLogs()
        for PV in PVs:
            # verify that the destination log entries are `tuple`
            assert PVs_[PV] == (detectorState.PVs[PV],)

    def test_roundedPVForState(self):
        # verify that `roundedPVForState` returns the rounded-to-resolution PVs
        schema = DetectorState.LEGACY_SCHEMA
        PVs_schema = DetectorState.LEGACY_SCHEMA["properties"]
        run, info = next(iter(self.runInfo.items()))
        PVs = self._logs_from_DetectorState(info["detectorState"])

        # Type normalization is applied to the stored PV values.
        type_normalized_PVs = {
            k: DetectorState._normalize_type(v, type_=PVs_schema[k]["type"], name=k) for k, v in PVs.items()
        }

        # Rounding-to-resolution is additionally applied to stored PV values as used in the hash computation.
        rounded_PVs = {
            k: DetectorState._round_to_resolution(
                v, resolution=PVs_schema[k].get("resolution", 0.0), use_legacy_rounding=schema["use_legacy_rounding"]
            )
            for k, v in type_normalized_PVs.items()
        }

        verify_uses_rounded = False
        verify_rounded_is_different = False
        detectorState = DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA)
        for PV in PVs:
            rounded_PV = detectorState.roundedPVForState(PV, DetectorState.LEGACY_SCHEMA)
            assert rounded_PV == rounded_PVs[PV]
            if rounded_PVs[PV] != detectorState.PVs[PV]:
                verify_uses_rounded = True
                verify_rounded_is_different = True
        assert verify_uses_rounded
        assert verify_rounded_is_different

    def test_pvLogsKey(self):
        # verify that the <derived PV> keys use the rounded-to-resolution values
        schema = DetectorState.LEGACY_SCHEMA
        PVs_schema = schema["properties"]
        run, info = next(iter(self.runInfo.items()))
        PVs = self._logs_from_DetectorState(info["detectorState"])

        detectorState = DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA)
        expected = (
            DetectorState._round_to_resolution(
                detectorState.PVs["det_arc1"],
                PVs_schema["det_arc1"].get("resolution", 0.0),
                use_legacy_rounding=schema["use_legacy_rounding"],
            ),
            DetectorState._round_to_resolution(
                detectorState.PVs["BL3:Chop:Skf1:WavelengthUserReq"],
                PVs_schema["BL3:Chop:Skf1:WavelengthUserReq"].get("resolution", 0.0),
                use_legacy_rounding=schema["use_legacy_rounding"],
            ),
        )
        unrounded_key = (detectorState.PVs["det_arc1"], detectorState.PVs["BL3:Chop:Skf1:WavelengthUserReq"])
        assert unrounded_key != expected

        actual = detectorState.pvLogsKey(("det_arc1", "BL3:Chop:Skf1:WavelengthUserReq"), DetectorState.LEGACY_SCHEMA)
        assert actual == expected

    def test_legacy_properties(self):
        # verify that the legacy properties return the correct PVs
        run, info = next(iter(self.runInfo.items()))
        PVs = self._logs_from_DetectorState(info["detectorState"])
        detectorState = DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA)

        assert detectorState.arc == (PVs["det_arc1"], PVs["det_arc2"])
        assert detectorState.wav == PVs["BL3:Chop:Skf1:WavelengthUserReq"]
        # `freq` is special, see previous comments
        assert detectorState.freq == int(round(PVs["BL3:Det:TH:BL:Frequency"]))
        assert detectorState.guideStat == PVs["BL3:Mot:OpticsPos:Pos"]
        assert detectorState.lin == (PVs["det_lin1"], PVs["det_lin2"])

    def test__round_to_resolution(self):
        # float value
        r = 1.2345
        # `List[float]`
        rs = [r, r + 1.1, r + 1.2]
        # `List[List[float]]`
        rss = [rs, rs, rs]
        # `int`
        n = 12345
        # `List[int]`
        ns = [n, n + 1, n + 2]
        # `List[List[int]]`
        nss = [ns, ns, ns]
        s = "12345"
        # `List[str]`
        ss = [s + "_1", s + "_2", s + "_3"]
        # `List[List[str]]`
        sss = [ss, ss, ss]

        # resolution == 0 => return the unmodified value
        for v in (r, rs, rss, n, ns, nss, s, ss, sss):
            assert DetectorState._round_to_resolution(v, 0.0) == v

        # resolution >= 0.0
        with pytest.raises(RuntimeError, match=".*resolution from schema.*abs_tol > 0.0.*"):
            DetectorState._round_to_resolution(r, -0.5)

        # resolution != 0.0: only for `float`, `List[float]`, `List[List... float]`
        for v in (n, ns, nss, s, ss, sss):
            with pytest.raises(RuntimeError, match=".*resolution from schema not implemented for type.*"):
                DetectorState._round_to_resolution(v, 0.5)

        # scalar `float`
        assert DetectorState._round_to_resolution(r, 0.1) == 1.2

        # `List[float]`
        expected = [DetectorState._round_to_resolution(v, 0.2) for v in rs]
        assert DetectorState._round_to_resolution(rs, 0.2) == expected

        # `List[List[float]]`
        expected = np.vectorize(lambda v_: DetectorState._round_to_resolution(v_, 0.3))(rss).tolist()
        assert DetectorState._round_to_resolution(rss, 0.3) == expected

    def test_hashcodes_independent_of_PVs_order(self):
        # computed hashcode does not depend on the order of the PVs in the logs:
        #   it does depend on order, but the order is enforced by the schema
        for run, info in self.runInfo.items():
            expectedHash = info["hash"]
            PVs = self._logs_from_DetectorState(info["detectorState"])
            PVss = permutations(PVs.items())
            # Taking only 5 is kind of arbitrary, but there are (7!) total!
            for n in range(5):
                PVs_ = dict(next(PVss))
                assert DetectorState.fromPVLogs(PVs_, DetectorState.LEGACY_SCHEMA).stateId.hex == expectedHash

    def test_hashcodes_depend_on_schema_order(self):
        # computed hashcode depends on the order of the PV-keys in the schema:
        legacy_properties = DetectorState.LEGACY_SCHEMA["properties"]

        propertiess = permutations(legacy_properties.items())
        verified_no_match = False

        # Now we need to check 10 permutations: the ignored PVs are at the end of the logs.
        for n in range(10):
            properties = dict(next(propertiess))
            with mock.patch.dict(DetectorState.LEGACY_SCHEMA, {"properties": properties}):
                for run, info in self.runInfo.items():
                    expectedHash = info["hash"]

                    # not all PVs are actually used in the state-ID hash computation
                    legacy_properties_keys = [
                        k for k in legacy_properties.keys() if not legacy_properties[k].get("ignore", False)
                    ]
                    properties_keys = [k for k in properties.keys() if not legacy_properties[k].get("ignore", False)]

                    PVs = self._logs_from_DetectorState(info["detectorState"])
                    if properties_keys == legacy_properties_keys:
                        assert DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA).stateId.hex == expectedHash
                    else:
                        assert DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA).stateId.hex != expectedHash
                        verified_no_match = True
        assert verified_no_match

    def test_hashcodes_ignore(self):
        # hashcode computation does not include PVs marked with "ignore" in the schema
        for run, info in self.runInfo.items():
            expectedHash = info["hash"]
            original_PVs = self._logs_from_DetectorState(info["detectorState"])
            PVs = original_PVs.copy()
            PVs.update({"det_lin1": 123.45, "det_lin2": 678.910})
            assert PVs != original_PVs
            assert DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA).stateId.hex == expectedHash

    def test_hashcodes_alias(self):
        # hashcode computation uses "alias" when included in the sub-schema for a PV
        PVs_schema = DetectorState.LEGACY_SCHEMA["properties"]

        verify_alias = False
        verify_no_alias = False
        verify_ignore = False

        # Here we modify the 'det_arc1' and 'det_arc2' entries so that they
        #   no longer have the aliases of 'vdet_arc1' and 'vdet_arc2'.
        #   This allows verification of the no-alias case.
        properties = DetectorState.LEGACY_SCHEMA["properties"].copy()
        properties.update(
            {
                "det_arc1": {
                    "type": "number",
                    "resolution": 0.5,
                },
                "det_arc2": {
                    "type": "number",
                    "resolution": 0.5,
                },
            }
        )

        with mock.patch.dict(DetectorState.LEGACY_SCHEMA, {"properties": properties}):
            for run, info in self.runInfo.items():
                expectedHash = info["hash"]
                PVs = self._logs_from_DetectorState(info["detectorState"])
                # At initialization, PVs are normalized by schema type:
                #   mostly this affects the _special_ rounding of 'BL3:Det:TH:BL:Frequency' from `float` to `int`.
                PVs = {k: DetectorState._normalize_type(v, PVs_schema[k]["type"], k) for k, v in PVs.items()}

                SHA = DetectorState.SHA(PVs, DetectorState.LEGACY_SCHEMA)
                dict_ = json.loads(SHA.decodedKey)

                print(f"-- dict_: {dict_}, decoded_key: {SHA.decodedKey}")

                # This is a reality check: the hash should not match, because
                #   we've changed the names used for the 'det_arc1' and 'det_arc2' entries.
                assert SHA.hex != expectedHash

                for k in DetectorState.LEGACY_SCHEMA["properties"]:
                    PV_schema = DetectorState.LEGACY_SCHEMA["properties"][k]
                    if not PV_schema.get("ignore", False):
                        assert PV_schema.get("alias", k) in dict_
                        if "alias" in PV_schema:
                            verify_alias = True
                        else:
                            verify_no_alias = True
                    else:
                        assert PV_schema.get("alias", k) not in dict_
                        verify_ignore = True
        assert verify_alias
        assert verify_no_alias
        assert verify_ignore

    def test_hashcodes_length(self):
        # verify that the schema 'length' field works correctly:
        #   note that `"length": 16` is implicitly verified elsewhere

        test_length = 24
        with mock.patch.dict(DetectorState.LEGACY_SCHEMA, {"length": test_length}):
            for run, info in self.runInfo.items():
                expectedHash = info["hash"]
                assert len(expectedHash) == 16
                assert len(expectedHash) < test_length

                PVs = self._logs_from_DetectorState(info["detectorState"])
                SHA = DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA).stateId
                assert len(SHA.hex) == test_length
                assert SHA.hex.startswith(expectedHash)

    def test_legacy_read(self):
        # test readback from legacy JSON
        legacy_JSON = """
        {{
            "arc": [
                {arc0},
                {arc1}
            ],
            "wav": {wav},
            "freq": {freq},
            "guideStat": {guideStat},
            "lin": [
                {lin0},
                {lin1}
            ]
        }}
        """
        with TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpPath:
            for run, info in self.runInfo.items():
                expectedHash = info["hash"]
                tempFilePath = Path(tmpPath) / f"detectorState_{expectedHash}.json"
                PVs = self._logs_from_DetectorState(info["detectorState"])
                expected = DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA)
                JSON = legacy_JSON.format(
                    arc0=expected["det_arc1"],
                    arc1=expected["det_arc2"],
                    wav=expected["BL3:Chop:Skf1:WavelengthUserReq"],
                    freq=expected["BL3:Det:TH:BL:Frequency"],
                    guideStat=expected["BL3:Mot:OpticsPos:Pos"],
                    lin0=expected["det_lin1"],
                    lin1=expected["det_lin2"],
                )
                # write
                with open(tempFilePath, "w") as f:
                    # verify that the JSON has the legacy form
                    assert "PVs" not in JSON
                    assert "stateId" not in JSON
                    f.write(JSON)
                # readback
                with open(tempFilePath, "r") as f:
                    actual = DetectorState.model_validate_json(f.read())

                # verify that `stateId` field has been added
                assert hasattr(actual, "stateId")

                # verify the complete conversion
                assert hasattr(actual, "PVs")
                assert actual == expected

    def test_read(self):
        # test readback from new-format JSON
        with TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpPath:
            for run, info in self.runInfo.items():
                expectedHash = info["hash"]
                tempFilePath = Path(tmpPath) / f"detectorState_{expectedHash}.json"
                PVs = self._logs_from_DetectorState(info["detectorState"])
                expected = DetectorState.fromPVLogs(PVs, DetectorState.LEGACY_SCHEMA)
                # write
                with open(tempFilePath, "w") as f:
                    JSON = expected.model_dump_json()
                    # verify that the JSON has the new form
                    assert "PVs" in JSON
                    assert "stateId" in JSON
                    f.write(JSON)
                # readback
                with open(tempFilePath, "r") as f:
                    actual = DetectorState.model_validate_json(f.read())
                assert actual == expected
