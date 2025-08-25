from typing import Any, Dict

from pydantic import ConfigDict, Field, PrivateAttr, model_validator

from snapred.backend.dao.indexing.IndexedObject import IndexedObject
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.meta.Config import Config


class InstrumentConfig(IndexedObject):
    """Class to hold the instrument parameters."""

    facility: str
    name: str
    nexusFileExtension: str
    nexusFilePrefix: str
    calibrationFileExtension: str
    calibrationFilePrefix: str
    calibrationDirectory: str
    pixelGroupingDirectory: str
    sharedDirectory: str
    nexusDirectory: str
    reducedDataDirectory: str
    reductionRecordDirectory: str
    bandwidth: float
    maxBandwidth: float
    L1: float
    L2: float
    delTOverT: float
    delLOverL: float
    width: float
    frequency: float
    lowWavelengthCrop: float = Field(default_factory=lambda: Config["constants.CropFactors.lowWavelengthCrop"])

    # attributes supporting extensible `DetectorState`:
    stateIdSchema: Dict[str, Any] = DetectorState.LEGACY_SCHEMA

    _derivedPVMap: Dict[str, Any] = PrivateAttr(default=None)

    def derivedPV(self, name: str, detectorState: DetectorState) -> Any:
        # Calculate the value of a derived PV using the schema:
        #   a derived PV depends on the values of other (possibly multiple) PVs.
        if name not in self._derivedPVMap:
            raise RuntimeError(f"the derived PV '{name}' is not present in the schema")
        try:
            schema = self._derivedPVMap[name]
            key = detectorState.pvLogsKey(schema["keyPVs"], self.stateIdSchema)
            try:
                value = schema["kvs"][key]
            except KeyError as e:
                raise RuntimeError(f"the PVs key '{e}', required to calculate '{name}' is not present in the schema")
        except KeyError as e:
            raise RuntimeError(f"the PV '{e}', required to calculate '{name}' is not present in the logs")
        return value

    """
    @classmethod
    def _scalarPV(cls, v: Any) -> Any:
        # preprocess a scalar PV value
        if not isinstance(v, (str, bytes, np.bytes_)) and isinstance(v, Iterable):
            # access the starting value from any time-series log
            v = v[0]
        if isinstance(v, (bytes, np.bytes_)):
            # decode fixed length bytes to "utf-8" string
            v = v.decode("utf-8")
        elif isinstance(v, Integral) and not isinstance(v, int):
            v = int(v)
        elif isinstance(v, Real) and not isinstance(v, float):
            v = float(v)
        else:
            raise ValueError(f"unexpected PV type {type(v)}")
        return v
    """

    # TODO: this duplicates `_list_to_tuple` at `DetectorState`!
    @classmethod
    def _list_to_tuple(cls, obj):
        # convert an arbitrarily-nested list to a hashable key
        if isinstance(obj, list):
            return tuple(cls._list_to_tuple(item) for item in obj)
        return obj

    @model_validator(mode="after")
    def _initDerivedPVMap(self):
        # reconstruct the derived-PV map from the schema into a more convenient format
        map_ = {}
        for name, sc in self.stateIdSchema["derivedPVs"].items():
            keyPVs = sc["keyPVs"]
            kvs = [(self._list_to_tuple(k), v) for k, v in sc["items"]]
            map_[name] = {"keyPVs": keyPVs, "kvs": dict(kvs)}
        self._derivedPVMap = map_
        return self

    # `extra="allow"` to support reading legacy instrument config
    model_config = ConfigDict(extra="allow")
