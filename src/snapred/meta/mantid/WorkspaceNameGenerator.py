from snapred.meta.Config import Config


class WorkspaceName(str):
    def __str__(self):
        return self


class _WorkspaceNameGenerator:
    _templateRoot = "mantid.workspace.nameTemplate"
    _runTemplate = Config[f"{_templateRoot}.run"]
    _diffCalInputTemplate = Config[f"{_templateRoot}.diffCal.input"]
    _diffCalTableTemplate = Config[f"{_templateRoot}.diffCal.table"]

    class Units:
        _templateRoot = "mantid.workspace.nameTemplate.units"
        DSP = Config[f"{_templateRoot}.dSpacing"]
        TOF = Config[f"{_templateRoot}.timeOfFlight"]

    class Groups:
        _templateRoot = "mantid.workspace.nameTemplate.groups"
        ALL = Config[f"{_templateRoot}.all"]
        COLUMN = Config[f"{_templateRoot}.column"]
        BANK = Config[f"{_templateRoot}.bank"]

    # TODO: Return abstract WorkspaceName type to help facilitate control over workspace names
    #       and discourage non-standard names.
    def run(self, runNumber, auxilary="", unit=Units.TOF, group=Groups.ALL):
        return WorkspaceName(self._runTemplate.format(unit=unit, group=group, auxilary=auxilary, runNumber=runNumber))

    def diffCalInput(self, runNumber, unit=Units.TOF):
        return WorkspaceName(self._diffCalInputTemplate.format(unit=unit, runNumber=runNumber))

    def diffCalTable(self, runNumber):
        return WorkspaceName(self._diffCalTableTemplate.format(runNumber=runNumber))


WorkspaceNameGenerator = _WorkspaceNameGenerator()
