from typing import Dict, List

from snapred.meta.Config import Config


class WorkspaceName(str):
    def __str__(self):
        return self


class NameBuilder:
    def __init__(self, template: str, keys: List[str], **kwargs):
        self.template = template
        self.keys = keys
        self.props = kwargs

    def __getattr__(self, key):
        if key not in self.keys:
            raise RuntimeError(f"Key [{key}] not a valid property for given name.")

        def setValue(value):
            self.props[key] = value
            return self

        return setValue

    def build(self):
        return self.template.format(**self.props)


class _WorkspaceNameGenerator:
    _templateRoot = "mantid.workspace.nameTemplate"
    _runTemplate = Config[f"{_templateRoot}.run"]
    _runTemplateKeys = ["runNumber", "auxilary", "lite", "unit", "group"]
    _diffCalInputTemplate = Config[f"{_templateRoot}.diffCal.input"]
    _diffCalInputTemplateKeys = ["runNumber", "unit"]
    _diffCalTableTemplate = Config[f"{_templateRoot}.diffCal.table"]
    _diffCalTableTemplateKeys = ["runNumber"]

    class Units:
        _templateRoot = "mantid.workspace.nameTemplate.units"
        DSP = Config[f"{_templateRoot}.dSpacing"]
        TOF = Config[f"{_templateRoot}.timeOfFlight"]

    class Groups:
        _templateRoot = "mantid.workspace.nameTemplate.groups"
        ALL = Config[f"{_templateRoot}.all"]
        COLUMN = Config[f"{_templateRoot}.column"]
        BANK = Config[f"{_templateRoot}.bank"]

    class Lite:
        TRUE = "Lite"
        FALSE = ""

    # TODO: Return abstract WorkspaceName type to help facilitate control over workspace names
    #       and discourage non-standard names.
    def run(self):
        return NameBuilder(
            self._runTemplate,
            self._runTemplateKeys,
            auxilary="",
            unit=self.Units.TOF,
            group=self.Groups.ALL,
            lite=self.Lite.FALSE,
        )

    def diffCalInput(self):
        return NameBuilder(self._diffCalInputTemplate, self._diffCalInputTemplateKeys, unit=self.Units.TOF)

    def diffCalTable(self):
        return NameBuilder(self._diffCalTableTemplate, self._diffCalTableTemplateKeys)


WorkspaceNameGenerator = _WorkspaceNameGenerator()
