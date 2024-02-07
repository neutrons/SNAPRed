from typing import Dict, List

from snapred.meta.Config import Config


class WorkspaceName(str):
    def __str__(self):
        return self


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
            self.props[key] = value
            return self

        return setValue

    def build(self):
        tokens = self.template.format(**self.props).split(",")
        tokens = [token.lower() for token in tokens if token != ""]
        return WorkspaceName(self.delimiter.join(tokens))


class _WorkspaceNameGenerator:
    _templateRoot = "mantid.workspace.nameTemplate"
    _delimiter = Config[f"{_templateRoot}.delimiter"]
    _runTemplate = Config[f"{_templateRoot}.run"]
    _runTemplateKeys = ["runNumber", "auxiliary", "lite", "unit", "group"]
    _diffCalInputTemplate = Config[f"{_templateRoot}.diffCal.input"]
    _diffCalInputTemplateKeys = ["runNumber", "unit"]
    _diffCalTableTemplate = Config[f"{_templateRoot}.diffCal.table"]
    _diffCalTableTemplateKeys = ["runNumber", "version"]
    _diffCalOutputTemplate = Config[f"{_templateRoot}.diffCal.output"]
    _diffCalOutputTemplateKeys = ["runNumber", "version", "unit"]
    _diffCalMaskTemplate = Config[f"{_templateRoot}.diffCal.mask"]
    _diffCalMaskTemplateKeys = ["runNumber", "version"]
    _diffCalMetricTemplate = Config[f"{_templateRoot}.diffCal.metric"]
    _diffCalMetricTemplateKeys = ["runNumber", "version", "metricName"]
    _rawVanadiumTemplate = Config[f"{_templateRoot}.normCal.rawVanadium"]
    _rawVanadiumTemplateKeys = ["unit", "group", "runNumber"]
    _focusedRawVanadiumTemplate = Config[f"{_templateRoot}.normCal.focusedRawVanadium"]
    _focusedRawVanadiumTemplateKeys = ["unit", "group", "runNumber"]
    _smoothedFocusedRawVanadiumTemplate = Config[f"{_templateRoot}.normCal.smoothedFocusedRawVanadium"]
    _smoothedFocusedRawVanadiumTemplateKeys = ["unit", "group", "runNumber"]

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
            self._diffCalInputTemplate, self._diffCalInputTemplateKeys, self._delimiter, unit=self.Units.TOF
        )

    def diffCalTable(self):
        return NameBuilder(
            self._diffCalTableTemplate, self._diffCalTableTemplateKeys, self._delimiter, version=""
        )

    def diffCalOutput(self):
        return NameBuilder(
            self._diffCalOutputTemplate, self._diffCalOutputTemplateKeys, self._delimiter, unit=self.Units.TOF, version=""
        )

    def diffCalMask(self):
        return NameBuilder(
            self._diffCalMaskTemplate, self._diffCalMaskTemplateKeys, self._delimiter, version=""
        )

    def diffCalMetrics(self):
        return NameBuilder(
            self._diffCalMetricTemplate, self._diffCalMetricTemplateKeys, self._delimiter, version=""
        )

    def rawVanadium(self):
        return NameBuilder(
            self._rawVanadiumTemplate,
            self._rawVanadiumTemplateKeys,
            self._delimiter,
            unit=self.Units.TOF,
            group=self.Groups.UNFOC,
        )

    def focusedRawVanadium(self):
        return NameBuilder(
            self._focusedRawVanadiumTemplate, self._focusedRawVanadiumTemplateKeys, self._delimiter, unit=self.Units.DSP
        )

    def smoothedFocusedRawVanadium(self):
        return NameBuilder(
            self._smoothedFocusedRawVanadiumTemplate,
            self._smoothedFocusedRawVanadiumTemplateKeys,
            self._delimiter,
            unit=self.Units.DSP,
        )


WorkspaceNameGenerator = _WorkspaceNameGenerator()
