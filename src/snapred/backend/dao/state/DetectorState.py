import math
from collections.abc import Iterable, Mapping
from enum import IntEnum
from numbers import Number, Real
from typing import Any, ClassVar, Dict, List, Tuple

import h5py
import numpy as np
from pydantic import BaseModel, model_validator

from snapred.backend.dao.ObjectSHA import ObjectSHA


class _LegacyGuideStatePos(IntEnum):
    IN = 1
    OUT = 2


class DetectorState(BaseModel):
    # For the generized state (see the schema), for each PV we retain its <value> [, <resolution>, [<alias>]]:
    #   <value>      ::= the original PV value from the logs, normalized to the schema type,
    #                    but not yet rounded to the target resolution,
    #   <resolution> ::= absolute resolution to be used in the state-ID hash computation for this PV,
    #   <alias>      ::= key name to be used in the hash computation.
    PVs: dict[str, Any]

    stateId: ObjectSHA | None

    ##
    ## `Mapping` methods:
    ##
    def __getitem__(self, key: str) -> Any:
        return self.PVs[key]

    def __setitem__(self, _key: str, _value: Any):
        raise RuntimeError("usage error: `DetectorState` is read only.")

    def keys(self) -> Iterable:
        return self.PVs.keys()

    def items(self) -> Iterable[Tuple[str, Any]]:
        return self.PVs.items()

    def values(self) -> Iterable[Any]:
        return self.PVs.values()

    def len(self) -> int:
        return self.PVs.len()

    ## end: `Mapping` methods.

    @classmethod
    def _round_to_resolution(cls, v: Any, resolution: float, use_legacy_rounding=False) -> Any:
        # Round a floating-point value to a specified resolution.
        if resolution != 0.0:
            if resolution < 0.0:
                raise RuntimeError("resolution from schema only defined for abs_tol > 0.0")
            if isinstance(v, float):
                rounded = round(v / resolution) * resolution
                if not use_legacy_rounding:
                    # ensure that an exact value is returned
                    decimal_places = max(0, -int(math.log10(resolution)) + 1) if resolution < 1.0 else 0
                    rounded = round(rounded, decimal_places)
                return rounded
            elif isinstance(v, list):
                return np.vectorize(lambda v_: DetectorState._round_to_resolution(v_, resolution, use_legacy_rounding))(
                    np.array(v)
                ).tolist()
            else:
                raise RuntimeError(f"resolution from schema not implemented for type '{type(v)}'")
        # Fallback: resolution == 0.0 => return the unmodified value.
        return v

    @classmethod
    def _normalize_type(cls, v: Any, type_: str, name: str) -> Number | str | List[Number] | List[str]:
        # Normalize a value's type so that it is consistently JSON-serializable:
        # -- convert any `numpy.ndarray` to nested list;
        # -- convert `numpy` scalar types to Python-native types;
        # -- when the target type in the schema is a scalar and the PV is not a scalar,
        #    take the first element (e.g. for time-series PV).

        if type_ == "array":
            # v should be either an `h5py.Dataset` or a `boost::python` wrapped `std::vector`
            if isinstance(v, h5py.Dataset):
                # access as `numpy.ndarray`
                v = v[...]
        elif not isinstance(v, (str, bytes, np.bytes_)) and isinstance(v, Iterable):
            # if a scalar is required, access the starting value from any time-series log
            v = v[0]

        # the following converts scalar types to Python-native types
        match type_:
            case "boolean":
                if type(v) is not bool:
                    v = bool(v)
            case "number":
                # Explanation for the syntax: `isinstance(np.float64, float)` returns True:
                #   `type(np.float64) is float` returns False.
                if type(v) is not float:
                    if not isinstance(v, Real):
                        raise ValueError(f"expecting PV '{name}' to be float not {type(v)}")
                    v = float(v)
            case "integer":
                if type(v) is not int:
                    # this special treatment is required by the legacy state, which occasionally converted
                    #   rounded `float` PV to `int`
                    if not isinstance(v, Number):
                        raise ValueError(f"expecting PV '{name}' to be a number not {type(v)}")
                    if isinstance(v, Real):
                        v = round(v)
                    v = int(v)
            case "string":
                if not isinstance(v, str):
                    if not isinstance(v, (bytes, np.bytes_)):
                        raise ValueError(f"expecting PV '{name}' to be string not {type(v)}")
                    # decode fixed length bytes to "utf-8" string
                    v = v.decode("utf-8")
            case "array":
                if not isinstance(v, list):
                    if not isinstance(v, Iterable):
                        raise ValueError(f"expecting PV '{name}' to be a time-series log not {type(v)}")
                    if isinstance(v, np.ndarray):
                        # Input from `h5py.Dataset` as `numpy.ndarray`:
                        # -- this treatment should correctly deal with any `ndarray`, not just
                        #    rank == 1.
                        if not v.dtype.type == np.bytes_:
                            # decode an `numpy.ndarray` to a (possibly nested) list of native Python scalar
                            v = v.tolist()
                        else:
                            # decode an `np.ndarray` of bytes to a (possibly nested) list of "utf-8" string
                            v = v.astype("U").tolist()
                    else:
                        # Input from `mantid.api.Run` as boost::python wrapped `std::vector`:
                        # -- at present, this only works with rank == 1: time-series vectors.
                        v = [s for s in v]
            case _:
                raise ValueError(f"'DetectorState' generalized schemas are not implemented for type '{type_}'")

        return v

    @classmethod
    def _get_resolution(cls, schema, key: str, default_value: float) -> float:
        # get the resolution corresponding to any PV:
        #   if the PV is a (possibly nested) array, descend to the deepest level
        if schema[key]["type"] == "array":
            return cls._get_resolution(schema[key], "items", default_value)
        return schema[key].get("resolution", default_value)

    @classmethod
    def SHA(cls, PVs: Dict[str, Any], schema: Dict[str, Any]) -> ObjectSHA:
        # Calculate a hash corresponding to these specific PV values.

        PVs_schema = schema["properties"]

        # Rounding to resolution is applied only _here_, thereby allowing
        #   calculations using this detector state to use the unrounded PV values.
        PVs_ = {
            PVs_schema[k].get("alias", k): cls._round_to_resolution(
                v, cls._get_resolution(PVs_schema, k, 0.0), schema.get("use_legacy_rounding", False)
            )
            for k, v in PVs.items()
            if not PVs_schema[k].get("ignore", False)
        }
        return ObjectSHA.fromObject(PVs_, length=schema["length"])

    def roundedPVForState(self, PV: str, schema: Dict[str, Any]) -> Any:
        # Calculate the rounded PV-value as used to compute the state-ID hash.

        PVs_schema = schema["properties"]
        return self._round_to_resolution(
            self.PVs[PV], self._get_resolution(PVs_schema, PV, 0.0), schema.get("use_legacy_rounding", False)
        )

    @model_validator(mode="before")
    @classmethod
    def _convertFromLegacyDetectorState(cls, v: Any) -> Any:
        if isinstance(v, dict) and "PVs" not in v:
            schema = cls.LEGACY_SCHEMA
            try:
                PVs = {
                    "det_arc1": v["arc"][0],
                    "det_arc2": v["arc"][1],
                    "BL3:Chop:Skf1:WavelengthUserReq": v["wav"],
                    "BL3:Det:TH:BL:Frequency": v["freq"],
                    "BL3:Mot:OpticsPos:Pos": v["guideStat"],
                    "det_lin1": v["lin"][0],
                    "det_lin2": v["lin"][1],
                }

                # match `fromPVLogs` behavior:
                #   as a special case, this will round the frequency PV, because that PV is a `float`
                #   but it is an 'integer' as used by the schema.
                PVs = {k: cls._normalize_type(v, schema["properties"][k]["type"], k) for k, v in PVs.items()}

                stateId = cls.SHA(PVs, schema)
                v = {"PVs": PVs, "stateId": stateId}
            except KeyError as e:
                raise ValueError(f"field '{e}' is missing from legacy `DetectorState`")
        return v

    @classmethod
    def fromPVLogs(cls, logs: Mapping[str, Any], schema: Dict[str, Any]) -> "DetectorState":
        PVs = {}
        for PV in schema["properties"]:
            if PV not in logs:
                raise RuntimeError(f"a required state PV '{PV}' is not present in the PV logs")
            field = schema["properties"][PV]
            value = cls._normalize_type(logs[PV], field["type"], PV)
            # resolution is not _applied_ at this point, this allows the `DetectorState` to be
            #   used in calculations which require PV values at the original precision.
            PVs[PV] = value
        stateId = cls.SHA(PVs, schema)
        return DetectorState(PVs=PVs, stateId=stateId)

    def toPVLogs(self) -> Dict[str, Any]:
        # This method is provided mostly to facilitate testing:
        #   it produces a set of logs as if the original time-series PVs are present.
        return {k: (v,) for k, v in self.PVs.items()}

    @classmethod
    def _list_to_tuple(cls, obj):
        # convert an arbitrarily-nested list to a hashable key
        if isinstance(obj, list):
            return tuple(cls._list_to_tuple(item) for item in obj)
        return obj

    def pvLogsKey(self, PVs: Iterable[str], schema: Dict[str, Any]) -> Tuple[Any, ...]:
        # Assemble a PVs key using the rounded PV values (as used in the hash calculation):
        #   this key will be used by `InstrumentConfig.derivedPV`.
        key = []
        PVs_schema = schema["properties"]
        for PV in PVs:
            resolution = self._get_resolution(PVs_schema, PV, 0.0)
            key.append(self._round_to_resolution(self.PVs[PV], resolution, schema.get("use_legacy_rounding", False)))
        return self._list_to_tuple(key)

    ##
    ## property aliases for legacy values
    ##
    @property
    def arc(self) -> Tuple[float, float]:
        return (self["det_arc1"], self["det_arc2"])

    @property
    def wav(self) -> float:
        return self["BL3:Chop:Skf1:WavelengthUserReq"]

    @property
    def freq(self) -> float:
        return self["BL3:Det:TH:BL:Frequency"]

    @property
    def guideStat(self) -> int:
        return self["BL3:Mot:OpticsPos:Pos"]

    @property
    def lin(self) -> Tuple[float, float]:
        return (self["det_lin1"], self["det_lin2"])

    LEGACY_SCHEMA: ClassVar[Dict[str, Any]] = {
        # This JSON schema represents the legacy state-id hash computation,
        # and also provides an example of the syntax used to potentially extend the state-Id:
        # -- Any PVlogs used must also be present as primary keys in `[Config["instrument.PVLogs.instrumentKeys"]`;
        # -- What is supported by the syntax is extensible, provided that it does not change the hash results
        #    for the legacy states.
        "type": "object",
        # number of hex digits in the hash string:
        #   increasing this value will add new hex digits to the tail
        "length": 16,
        # The legacy State-ID hash calculation used imprecise rounding:
        #   set this flag in order to duplicate that behavior.  Do NOT include or set this
        #   flag for non-legacy schemas!
        "use_legacy_rounding": True,
        "properties": {
            # ORDER OF PROPERTIES MATTERS:
            #   changing the order below will change the resulting hash string!
            # PVlogs key
            "det_arc1": {
                # Implemented types (at present): "number", "integer", "boolean", "string", and "array":
                # -- any type supported by HDF5, with the exception of compound types, is allowed;
                # -- use "number" for `float`.
                "type": "number",
                # [optional] Resolution which will be included in the hash computation:
                #   this is essentially the same as an <absolute tolerance>,
                #   but it will be used for rounding.
                "resolution": 0.5,
                # [optional] Attribute name used in the hash computation:
                # -- only include if different from the PVlogs key;
                # -- this field can also be used for remapping, if for some reason the PVlogs key changes.
                "alias": "vdet_arc1",
            },
            ## Example of a `List[float]` attribute (also must be added to the "required" list).
            ## Array attributes may include arbitrarily-nested arrays.
            ##
            ## "PVLogs:key": {
            ##     "type": "array",
            ##     "items": {
            ##         "type": "number",
            ##         "resolution": 0.001
            ##     }
            ## },
            "det_arc2": {"type": "number", "resolution": 0.5, "alias": "vdet_arc2"},
            "BL3:Chop:Skf1:WavelengthUserReq": {"type": "number", "resolution": 0.1, "alias": "WavelengthUserReq"},
            "BL3:Det:TH:BL:Frequency": {
                # The PV is actually a floating-point value:
                #   the legacy state rounds this and converts it to an integer
                "type": "integer",
                "alias": "Frequency",
            },
            "BL3:Mot:OpticsPos:Pos": {"type": "integer", "alias": "Pos"},
            "det_lin1": {"type": "number", "ignore": True},
            "det_lin2": {"type": "number", "ignore": True},
        },
        "required": [
            # Use the PVlogs keys here, not the "alias".
            "det_arc1",
            "det_arc2",
            "BL3:Chop:Skf1:WavelengthUserReq",
            "BL3:Det:TH:BL:Frequency",
            "BL3:Mot:OpticsPos:Pos",
            "det_lin1",
            "det_lin2",
        ],
        # Here we place values which are derived from other (possibly multiple) PVs, such as `deltaTheta`.
        "derivedPVs": {
            "deltaTheta": {
                # the tuple of PVs used as the key:
                #   (`list`s should be used here to help with tests, as `tuple` won't convert through JSON)
                "keyPVs": [
                    "BL3:Mot:OpticsPos:Pos",
                ],
                # a list of key-value pairs used to form the map:
                #   representation as key-value pairs prevents JSON from stringifying the keys
                "items": [
                    [
                        [
                            _LegacyGuideStatePos.IN,
                        ],
                        6.40e-3,
                    ],
                    [
                        [
                            _LegacyGuideStatePos.OUT,
                        ],
                        2.00e-3,
                    ],
                ],
            }
        },
    }
