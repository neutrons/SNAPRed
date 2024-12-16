import re
import sys
from datetime import datetime
from enum import Enum
from typing import Any, List, NamedTuple, Optional, Tuple

from pydantic import WithJsonSchema
from pydantic.functional_validators import BeforeValidator
from typing_extensions import Annotated, Self

from snapred.backend.dao.indexing.Versioning import VERSION_START, VersionState
from snapred.meta.Config import Config


class WorkspaceName(str):
    def __new__(cls, value: Any) -> Self:
        # Allow use of `WorkspaceName` with `pydantic.BaseModel`:
        obj = super().__new__(cls, value)
        obj._builder = getattr(value, "_builder", None)
        return obj

    def __str__(self):
        return self

    def toString(self) -> str:
        # return an actual string, not a `WorkspaceName`
        return super(self.__class__, self).__str__()

    def tokens(self, *args: Tuple[str]):
        # return multiple token values:
        #   usage:
        #     multiple-value: runNumber, useLiteMode, version = <builder>.tokens("runNumber", "useLiteMode", "version")
        #     or single-value: runNumber = <builder>.tokens("runNumber")
        if self._builder is None:
            raise RuntimeError(f"no '_builder' attribute is retained for 'WorkspaceName' {self}")
        values = self._builder.tokens(*args)
        return tuple(values) if len(values) > 1 else values[0]

    @classmethod
    def instance(cls, value, builder=None):
        # factory method
        wsName = cls(value)
        wsName._builder = builder
        return wsName


### THIS IS A KLUGE: 'sphinx' does not like `typing.Annotated`
if "sphinx" not in sys.modules:
    _WorkspaceName = WorkspaceName
    WorkspaceName = Annotated[
        _WorkspaceName,
        BeforeValidator(lambda v: _WorkspaceName(v) if isinstance(v, str) else v),
        WithJsonSchema({"type": "string"}, mode="serialization"),
        WithJsonSchema({"type": "string"}, mode="validation"),
    ]


class WorkspaceType(str, Enum):
    # TODO: not yet completely connected: should be a mixin to the WNG class itself...
    #   For the moment: the <enum>.value will be `_WorkspaceNameGenerator` method name
    RUN = "run"
    DIFFCAL_INPUT = "diffCalInput"
    DIFFCAL_OUTPUT = "diffCalOutput"
    DIFFCAL_DIAG = "diffCalDiagnostic"
    DIFFCAL_TABLE = "diffCalTable"
    DIFFCAL_MASK = "diffCalMask"
    DIFFCAL_METRIC = "diffCalMetric"
    DIFFCAL_TIMED_METRIC = "diffCalTimedMetric"
    RAW_VANADIUM = "rawVanadium"
    FOCUSED_RAW_VANADIUM = "focusedRawVanadium"
    SMOOTHED_FOCUSED_RAW_VANADIUM = "smoothedFocusedRawVanadium"

    # <reduction tag>_<runNumber>_<timestamp>
    REDUCTION_OUTPUT = "reductionOutput"
    # <reduction tag>_<stateSHA>_<timestamp>
    REDUCTION_OUTPUT_GROUP = "reductionOutputGroup"
    # <reduction tag>_pixelmask_<runNumber>_<timestamp>
    REDUCTION_PIXEL_MASK = "reductionPixelMask"
    # MaskWorkspace_<number tag>
    REDUCTION_USER_PIXEL_MASK = "userPixelMask"


class NameBuilder:
    def __init__(self, wsType: WorkspaceType, template: str, keys: List[str], delimiter: str, **kwargs):
        self.template = template
        self.keys = keys
        self.props = kwargs
        self.delimiter = delimiter

        # retain 'workspaceType' property, for internal use:
        self.keys.append("workspaceType")
        self.props["workspaceType"] = wsType

    def __getattr__(self, key):
        if key not in self.keys:
            raise RuntimeError(f"Key '{key}' not a valid property for workspace-type '{self.props['workspaceType']}'.")

        def setValue(value):
            # IMPORTANT: in the builder, we retain the _unformatted_ values;
            #   formatting occurs only at the time of build.
            self.props[key] = value
            return self

        return setValue

    def tokens(self, *args: Tuple[str]):
        # return multiple token values:
        #   usage: runNumber, useLiteMode, version = <builder>.tokens("runNumber", "useLiteMode", "version")
        values = []
        for key in args:
            if key not in self.keys:
                raise RuntimeError(
                    f"Key '{key}' not a valid property for workspace-type '{self.props['workspaceType']}'."
                )
            values.append(self.props[key])
        return tuple(values)

    def build(self):
        formattedProperties = {key: ValueFormatter.formatValueByKey(key, value) for key, value in self.props.items()}
        tokens = self.template.format(**formattedProperties).split(",")
        tokens = [token for token in tokens if token != ""]
        return WorkspaceName.instance(self.delimiter.join(tokens), self)


class ValueFormatter:
    class FormatTuple(NamedTuple):
        WORKSPACE: str
        PATH: str

    versionFormat = FormatTuple(
        WORKSPACE=Config["mantid.workspace.nameTemplate.formatter.version.workspace"],
        PATH=Config["mantid.workspace.nameTemplate.formatter.version.path"],
    )
    timestampFormat = FormatTuple(
        WORKSPACE=Config["mantid.workspace.nameTemplate.formatter.timestamp.workspace"],
        PATH=Config["mantid.workspace.nameTemplate.formatter.timestamp.path"],
    )
    numberTagFormat = FormatTuple(
        WORKSPACE=Config["mantid.workspace.nameTemplate.formatter.numberTag.workspace"],
        PATH=Config["mantid.workspace.nameTemplate.formatter.numberTag.path"],
    )
    runNumberFormat = FormatTuple(
        WORKSPACE=Config["mantid.workspace.nameTemplate.formatter.runNumber.workspace"],
        PATH=Config["mantid.workspace.nameTemplate.formatter.runNumber.path"],
    )
    stateIdFormat = FormatTuple(
        WORKSPACE=Config["mantid.workspace.nameTemplate.formatter.stateId.workspace"],
        PATH=Config["mantid.workspace.nameTemplate.formatter.stateId.path"],
    )

    @classmethod
    def formatNumberTag(cls, number: int, fmt=numberTagFormat.WORKSPACE):
        # Mantid-workspace number tag: only resolves if number > 1
        if number == 1:
            return ""
        return fmt.format(number=number)

    @classmethod
    def pathNumberTag(cls, number: int):
        # Mantid-workspace number tag: only resolves if number > 1
        if number == 1:
            return ""
        return cls.formatNumberTag(number, fmt=cls.numberTagFormat.PATH)

    @classmethod
    def formatRunNumber(cls, runNumber: str, fmt=runNumberFormat.WORKSPACE):
        return fmt.format(runNumber=runNumber)

    @classmethod
    def pathRunNumber(cls, runNumber: str):
        return cls.formatRunNumber(runNumber=runNumber, fmt=cls.runNumberFormat.PATH)

    @classmethod
    def formatVersion(cls, version: Optional[int], fmt=versionFormat.WORKSPACE):
        # handle two special cases of unassigned or default version
        # in those cases, format will be a user-specified string

        formattedVersion = ""
        if version == VersionState.DEFAULT:
            formattedVersion = f"v{VERSION_START}"
        elif isinstance(version, int):
            formattedVersion = fmt.format(version=version)
        elif str(version).isdigit():
            formattedVersion = fmt.format(version=int(version))
        return formattedVersion

    @classmethod
    def pathVersion(cls, version: int):
        # only one special case: default version

        if version == VersionState.DEFAULT:
            return f"v_{VersionState.DEFAULT}"
        return cls.formatVersion(version, fmt=cls.versionFormat.PATH)

    @classmethod
    def formatTimestamp(cls, timestamp: Optional[float], fmt=timestampFormat.WORKSPACE):
        if timestamp is None:
            return ""
        if "timestamp" in fmt:
            return fmt.format(timestamp=int(round(timestamp * 1000.0)))
        return datetime.fromtimestamp(timestamp).strftime(fmt)

    @classmethod
    def pathTimestamp(cls, timestamp: float):
        return cls.formatTimestamp(timestamp, fmt=cls.timestampFormat.PATH)

    @classmethod
    def formatStateId(cls, stateId: str, fmt=stateIdFormat.WORKSPACE):
        return fmt.format(stateId=stateId)

    @classmethod
    def pathStateId(cls, stateId: str):
        return cls.formatStateId(stateId, fmt=cls.stateIdFormat.PATH)

    @staticmethod
    def formatValueByKey(key: str, value: any):
        match key:
            case "numberTag":
                value = ValueFormatter.formatNumberTag(value)
            case "runNumber":
                value = ValueFormatter.formatRunNumber(value)
            case "version":
                value = ValueFormatter.formatVersion(value)
            case "timestamp":
                value = ValueFormatter.formatTimestamp(value)
            case "stateId":
                value = ValueFormatter.formatStateId(value)
            case _:
                # IMPORTANT: moving the _lowercase_ conversion to this location enables case sensitivity
                #   in both formatter output and in literal tokens from the template itself.
                # This is now required, as Mantid itself uses capitalized names (e.g. "MaskWorkspace_2").
                value = str(value).lower() if value != "" else ""
        return value


class _WorkspaceNameGenerator:
    # TODO: define <workspace type> Enum

    _templateRoot = "mantid.workspace.nameTemplate"
    _delimiter = Config[f"{_templateRoot}.delimiter"]

    def __init__(self):
        self._setupTemplateVars(Config[f"{self._templateRoot}.template"])

    def _setupTemplateVars(self, templateDict, namePrefix=""):
        """
        Recursively sets up template variables from a dictionary
        that may contain nested dictionaries of templates.
        """
        for key, value in templateDict.items():
            name = key
            if namePrefix:
                name = f"{namePrefix}{self._capFirst(name)}"

            if isinstance(value, str):
                # is an instance of a template
                # parse!
                template = value
                self._setTemplateVar(template, name)
            else:
                # is another dictionary of more templates
                # recurse!
                self._setupTemplateVars(value, name)

    def _capFirst(self, s: str) -> str:
        return s[0].upper() + s[1:]

    def _setTemplateVar(self, template: str, name: str):
        setattr(self, f"_{name}Template", template)
        setattr(self, f"_{name}TemplateKeys", self._parseTemplateKeys(template))

    def _parseTemplateKeys(self, template: str) -> List[str]:
        # regex for strings between {}
        regex = r"\{([^\}]*)\}"
        return re.findall(regex, template)

    class Units:
        _templateRoot = "mantid.workspace.nameTemplate.units"
        DSP = Config[f"{_templateRoot}.dSpacing"]
        TOF = Config[f"{_templateRoot}.timeOfFlight"]
        QSP = Config[f"{_templateRoot}.momentumTransfer"]
        LAM = Config[f"{_templateRoot}.wavelength"]
        DIAG = Config[f"{_templateRoot}.diagnostic"]

    class Groups:
        _templateRoot = "mantid.workspace.nameTemplate.groups"
        ALL = Config[f"{_templateRoot}.all"]
        COLUMN = Config[f"{_templateRoot}.column"]
        BANK = Config[f"{_templateRoot}.bank"]
        UNFOC = Config[f"{_templateRoot}.unfocussed"]

    class Lite:
        TRUE = "lite"
        FALSE = ""

    # TODO: Return abstract WorkspaceName type to help facilitate control over workspace names
    #       and discourage non-standard names.
    def run(self):
        return NameBuilder(
            WorkspaceType.RUN,
            self._runTemplate,
            self._runTemplateKeys,
            self._delimiter,
            auxiliary="",
            unit=self.Units.TOF,
            group=self.Groups.ALL,
            lite=self.Lite.FALSE,
        )

    def diffCalInput(self):
        return NameBuilder(
            WorkspaceType.DIFFCAL_INPUT,
            self._diffCalInputTemplate,
            self._diffCalInputTemplateKeys,
            self._delimiter,
            unit=self.Units.TOF,
        )

    def diffCalInputDSP(self):
        return NameBuilder(
            WorkspaceType.DIFFCAL_INPUT,
            self._diffCalInputTemplate,
            self._diffCalInputTemplateKeys,
            self._delimiter,
            unit=self.Units.DSP,
        )

    def diffCalTable(self):
        return NameBuilder(
            WorkspaceType.DIFFCAL_TABLE,
            self._diffCalTableTemplate,
            self._diffCalTableTemplateKeys,
            self._delimiter,
            version=None,
        )

    def diffCalOutput(self):
        return NameBuilder(
            WorkspaceType.DIFFCAL_OUTPUT,
            self._diffCalOutputTemplate,
            self._diffCalOutputTemplateKeys,
            self._delimiter,
            unit=self.Units.TOF,
            group=self.Groups.UNFOC,
            version=None,
        )

    def diffCalDiagnostic(self):
        return NameBuilder(
            WorkspaceType.DIFFCAL_DIAG,
            self._diffCalDiagnosticTemplate,
            self._diffCalDiagnosticTemplateKeys,
            self._delimiter,
            group=self.Groups.UNFOC,
            version=None,
        )

    def diffCalMask(self):
        return NameBuilder(
            WorkspaceType.DIFFCAL_MASK,
            self._diffCalMaskTemplate,
            self._diffCalMaskTemplateKeys,
            self._delimiter,
            version=None,
        )

    def diffCalMetric(self):
        return NameBuilder(
            WorkspaceType.DIFFCAL_METRIC,
            self._diffCalMetricTemplate,
            self._diffCalMetricTemplateKeys,
            self._delimiter,
            version=None,
        )

    def diffCalTimedMetric(self):
        return NameBuilder(
            WorkspaceType.DIFFCAL_TIMED_METRIC,
            self._diffCalTimedMetricTemplate,
            self._diffCalTimedMetricTemplateKeys,
            self._delimiter,
        )

    def rawVanadium(self):
        return NameBuilder(
            WorkspaceType.RAW_VANADIUM,
            self._normCalRawVanadiumTemplate,
            self._normCalRawVanadiumTemplateKeys,
            self._delimiter,
            unit=self.Units.TOF,
            group=self.Groups.UNFOC,
            version=None,
        )

    def focusedRawVanadium(self):
        return NameBuilder(
            WorkspaceType.FOCUSED_RAW_VANADIUM,
            self._normCalFocusedRawVanadiumTemplate,
            self._normCalFocusedRawVanadiumTemplateKeys,
            self._delimiter,
            unit=self.Units.DSP,
            version=None,
        )

    def smoothedFocusedRawVanadium(self):
        return NameBuilder(
            WorkspaceType.SMOOTHED_FOCUSED_RAW_VANADIUM,
            self._normCalSmoothedFocusedRawVanadiumTemplate,
            self._normCalSmoothedFocusedRawVanadiumTemplateKeys,
            self._delimiter,
            unit=self.Units.DSP,
            version=None,
        )

    def reductionOutput(self):
        return NameBuilder(
            WorkspaceType.REDUCTION_OUTPUT,
            self._reductionOutputTemplate,
            self._reductionOutputTemplateKeys,
            self._delimiter,
            unit=self.Units.DSP,
            timestamp=None,
        )

    def reductionOutputGroup(self):
        return NameBuilder(
            WorkspaceType.REDUCTION_OUTPUT_GROUP,
            self._reductionOutputGroupTemplate,
            self._reductionOutputGroupTemplateKeys,
            self._delimiter,
            timestamp=None,
        )

    def reductionPixelMask(self):
        return NameBuilder(
            WorkspaceType.REDUCTION_PIXEL_MASK,
            self._reductionPixelMaskTemplate,
            self._reductionPixelMaskTemplateKeys,
            self._delimiter,
            timestamp=None,
        )

    def reductionUserPixelMask(self):
        return NameBuilder(
            WorkspaceType.REDUCTION_USER_PIXEL_MASK,
            self._reductionUserPixelMaskTemplate,
            self._reductionUserPixelMaskTemplateKeys,
            self._delimiter,
            numberTag=None,
        )


WorkspaceNameGenerator = _WorkspaceNameGenerator()
