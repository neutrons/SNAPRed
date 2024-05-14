import re
from enum import Enum
from typing import List, Optional

from snapred.meta.Config import Config


class WorkspaceName(str):
    def __str__(self):
        return self


class WorkspaceType(str, Enum):
    # TODO: not yet completely connected: should be a mixin to the WNG class itself...
    #   For the moment: the <enum>.value will be `_WorkspaceNameGenerator` method name
    RUN = "run"
    DIFFCAL_INPUT = "diffCalInput"
    DIFFCAL_OUTPUT = "diffCalOutput"
    DIFFCAL_TABLE = "diffCalTable"
    DIFFCAL_MASK = "diffCalMask"
    DIFFCAL_METRIC = "diffCalMetric"
    DIFFCAL_TIMED_METRIC = "diffCalTimedMetric"
    RAW_VANADIUM = "rawVanadium"
    FOCUSED_RAW_VANADIUM = "focusedRawVanadium"
    SMOOTHED_FOCUSED_RAW_VANADIUM = "smoothedFocusedRawVanadium"


class NameBuilder:
    def __init__(self, template: str, keys: List[str], delimiter: str, **kwargs):
        self.template = template
        self.keys = keys
        self.props = kwargs
        self.delimiter = delimiter

    def __getattr__(self, key):
        if key not in self.keys:
            raise RuntimeError(f"Key [{key}] not a valid property for given name.")

        def setValue(value):
            value = ValueFormatter.formatValueByKey(key, value)
            self.props[key] = value
            return self

        return setValue

    def build(self):
        tokens = self.template.format(**self.props).split(",")
        tokens = [token.lower() for token in tokens if token != ""]
        return WorkspaceName(self.delimiter.join(tokens))


class ValueFormatter:
    class vPrefix:
        WORKSPACE = True
        FILE = False

    @staticmethod
    def formatRunNumber(runNumber: str):
        return str(runNumber).zfill(6)

    @staticmethod
    def formatVersion(version: Optional[int], use_v_prefix: vPrefix = vPrefix.WORKSPACE):
        if version is None:
            return ""
        if not version == "*":
            version = str(version).zfill(4)
        prefix = "v" if use_v_prefix == ValueFormatter.vPrefix.WORKSPACE else "v_"
        return str(prefix + version)

    @staticmethod
    def formatTimestamp(timestamp: str):
        return "ts" + timestamp

    @staticmethod
    def formatValueByKey(key: str, value: any):
        if key == "runNumber":
            value = ValueFormatter.formatRunNumber(value)
        elif key == "version":
            value = ValueFormatter.formatVersion(value)
        elif key == "timestamp":
            value = ValueFormatter.formatTimestamp(value)
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
            self._diffCalInputTemplate,
            self._diffCalInputTemplateKeys,
            self._delimiter,
            unit=self.Units.TOF,
        )

    def diffCalTable(self):
        return NameBuilder(
            self._diffCalTableTemplate,
            self._diffCalTableTemplateKeys,
            self._delimiter,
            version="",
        )

    def diffCalOutput(self):
        return NameBuilder(
            self._diffCalOutputTemplate,
            self._diffCalOutputTemplateKeys,
            self._delimiter,
            unit=self.Units.TOF,
            group=self.Groups.UNFOC,
            version="",
        )

    def diffCalMask(self):
        return NameBuilder(
            self._diffCalMaskTemplate,
            self._diffCalMaskTemplateKeys,
            self._delimiter,
            version="",
        )

    def diffCalMetric(self):
        return NameBuilder(
            self._diffCalMetricTemplate,
            self._diffCalMetricTemplateKeys,
            self._delimiter,
            version="",
        )

    def diffCalTimedMetric(self):
        return NameBuilder(
            self._diffCalTimedMetricTemplate,
            self._diffCalTimedMetricTemplateKeys,
            self._delimiter,
        )

    def rawVanadium(self):
        return NameBuilder(
            self._normCalRawVanadiumTemplate,
            self._normCalRawVanadiumTemplateKeys,
            self._delimiter,
            unit=self.Units.TOF,
            group=self.Groups.UNFOC,
            version="",
        )

    def focusedRawVanadium(self):
        return NameBuilder(
            self._normCalFocusedRawVanadiumTemplate,
            self._normCalFocusedRawVanadiumTemplateKeys,
            self._delimiter,
            unit=self.Units.DSP,
            version="",
        )

    def smoothedFocusedRawVanadium(self):
        return NameBuilder(
            self._normCalSmoothedFocusedRawVanadiumTemplate,
            self._normCalSmoothedFocusedRawVanadiumTemplateKeys,
            self._delimiter,
            unit=self.Units.DSP,
            version="",
        )


WorkspaceNameGenerator = _WorkspaceNameGenerator()
