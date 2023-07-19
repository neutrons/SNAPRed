import os
from collections import namedtuple

from mantid.api import AlgorithmManager, Progress, mtd
from mantid.kernel import Direction

from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.log.logger import snapredLogger

# must import to register with AlgorithmManager
from snapred.meta.Callback import callback
from snapred.meta.Config import Resource

logger = snapredLogger.getLogger(__name__)


class _CustomMtd:
    def __getitem__(self, key):
        if str(key.__class__) == str(callback(int).__class__):
            key = key.get()
        return mtd[key]


class MantidSnapper:
    typeTranslationTable = {"string": str, "number": float, "dbl list": list, "boolean": bool}
    _mtd = _CustomMtd()

    def __init__(self, parentAlgorithm, name):
        """
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMWWMWNXWMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMWNNOdOkllxkkKNWWMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMW0ol:;:::::;:lodO0KWMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMN0o;;;;;::;;:::::::lxO0XNWMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMWOc:;;;;::;;:;:::::::;:clldO0XNNNNWWWMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMWNXKOo;;,,,,,,,;;;;;;::::::::::::clcclddxk0XWMMMMMMMMMMMMMMMMMMMMMMMMMMMMWWNW
        MMMMMMMMMMMMMMMMMMMNKOxdlcc:::cccccccccc::::;;:;;:::;;;;,,,,;;;,,;cdk0KNWMMMMMMMMMMMMMMMMMMWNX0kxlo0
        MMMMMMMMMMMMMMMNKOxl:;:cllooddxxxkkkOkkxxxddooollccc::;,,,,,,,,,,;;;;::ldOXWMMMMMMMMMMMMWXOxol::;,oX
        MMMMMMMMMMMMN0dlccccclodxxxxkkxkxxkkkxxkkkkkkkkkkxxxdoolc:;,,,,,,,,,,;;;;:dXWMMMMMMMMWXOdlcccc:;,:OW
        MMMMMMMMMWKxl;,;:clodxxkkkkxkkkkkkkkkkkkkkkkxxkkkkkkkkxxddollc:;;,,,,;;;:oONMMMMMMMN0dcccllcccc:,dNM
        MMMMMMWXko:;,,;:cloodxxkkkkOOOOOOOOOOOOkkkkkkOOOOOOOkkkkkkkxxxdolcc:;:cdOKNWWWWWNKOo:;:cccllccc;,xWM
        MMMWNOo::c:,..',cooooddkOOOOOOOOOOOOOOOOkOOOOOOOOOOOOOOOOOOOOkkkxxxdolclloddddddolcc::::clllccc;;kWM
        MNKxc;;:odo;'..;dxxxkOO0K000000000000000OOOOOOOO00000OO000000OOOOOkkkxxxxdddddxxxxdolc:cccccc::,,xWM
        0o:cloodxkkxoodk0OkO0KKKK000KKKKKK00OOOkkkkxxxkkxxkkk0KKKKKKKKKKKKK000000000OOOOOOkxdllcclllc:;..xWM
        0doodxkkkkkkOO00kkkO0K00K00OOOkkxdoooooooolllllodxOO0KXXXXXXXXXXXXXXKXKKKKKKKKKKK000Odlcccccc:,..kMM
        0xxdoodk0OkxxkkkxkkO0KXK00OdolccccllloooolllodkO0KXXXXXXXXXXXXXXXK0OkkxxxxxxxxxxkkOOkdlcccc:c:;';0MM
        WXKOkdddxkxxxxxkOOOO0000KKKOdllllloooooooodk0KXXNNNXXXXXKKXXKK00kxdodxO0KKKXXXK0Oxdooolccccc::;';0MM
        MMMWWXK0Okxxxxxkkkkkkk0XXXXKklccloodddddkOKKXXXXXXKXXKK00OOOkxdoollodxOXWMMMMMMMMWXOxoolcccc::;';OMM
        MMMMMMMMWNNXKOkxddoddk0000K0OdoooooddxkO00KK0000000OOkkddooolllllooodxxkXWMMMMMMMMMMWKkdlcc:::;,'dWM
        MMMMMMMMMMMMMMWNXKK00OOkkxxxxxddddddddxxxxxxxxddxxxxxxdollllllllloddxxddkXWMMMMMMMMMMMWN0xl:;;;,':0M
        MMMMMMMMMMMMMMMMMMMMMMWWWNXXKxlllldkOOO00000KKKXXNNNNWNKkdooooddooddxxdddkXWMMMMMMMMMMMMMMN0xo:,''oN
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMWXxlllodkXWMMMMMMMMMMMMMMMMMWNKO0K000OOOOOOO0KXNMMMMMMMMMMMMMMMMMWX0OxkX
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMXkollloONMMMMMMMMMMMMMMMMMMMMMMMMMMWMMMWMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMNkolloOWMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMN0dlo0WMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMWXOOXMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMWWMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        """
        self.parentAlgorithm = parentAlgorithm
        self._name = name
        self._endrange = 0
        self._progressCounter = 0
        self._prog_reporter = None
        self._algorithmQueue = []
        self._exportScript = ""
        self._export = False
        if self._export:
            self._cleanOldExport()

    def createOutputCallback(self, prop):
        callbackType = self.typeTranslationTable.get(prop.type, str)
        return callback(callbackType)

    def __getattr__(self, key):
        def enqueueAlgorithm(message, **kwargs):
            self._endrange += 1
            # inspect mantid algorithm for output properties
            # if there are any, add them to a list for return
            outputProperties = {}
            mantidAlgorithm = AlgorithmManager.create(key)
            # get all output props
            for prop in mantidAlgorithm.getProperties():
                if Direction.values[prop.direction] == Direction.Output:
                    outputProperties[prop.name] = self.createOutputCallback(prop)
            # get only set inout props
            for propName in set(kwargs.keys()).difference(mantidAlgorithm.getProperties()):
                prop = mantidAlgorithm.getProperty(propName)
                if Direction.values[prop.direction] == Direction.InOut:
                    outputProperties[prop.name] = self.createOutputCallback(prop)

            # TODO: Special cases are bad
            if key == "LoadDiffCal":
                if kwargs.get("MakeGroupingWorkspace", True):
                    outputProperties["GroupingWorkspace"] = kwargs["WorkspaceName"] + "_group"
                if kwargs.get("MakeMaskWorkspace", True):
                    outputProperties["MaskWorkspace"] = kwargs["WorkspaceName"] + "_mask"
                if kwargs.get("MakeCalWorkspace", True):
                    outputProperties["CalWorkspace"] = kwargs["WorkspaceName"] + "_cal"

            # remove mantid algorithm from managed algorithms
            AlgorithmManager.removeById(mantidAlgorithm.getAlgorithmID())
            # if only one property is returned, return it directly

            self._algorithmQueue.append((key, message, kwargs, outputProperties))

            if len(outputProperties) == 1:
                (outputProperties,) = outputProperties.values()
            else:
                # Convert to tuple for a more pythonic return
                NamedOutputsTuple = namedtuple("{}Outputs".format(key), outputProperties.keys())
                outputProperties = NamedOutputsTuple(**outputProperties)

            return outputProperties

        return enqueueAlgorithm

    def reportAndIncrement(self, message):
        if not self._prog_reporter:
            return
        self._prog_reporter.reportIncrement(self._progressCounter, message)
        self._progressCounter += 1

    def createAlgorithm(self, name):
        alg = AlgorithmManager.create(name)
        alg.setChild(True)
        alg.setAlwaysStoreInADS(True)
        alg.setRethrows(True)
        return alg

    def executeAlgorithm(self, name, outputs, **kwargs):
        algorithm = self.createAlgorithm(name)
        try:
            for prop, val in kwargs.items():
                # this line is to appease mantid properties, idk where its pulling empty string from
                if str(val.__class__) == str(callback(int).__class__):
                    val = val.get()
                algorithm.setProperty(prop, val)
            if not algorithm.execute():
                raise RuntimeError("")
            for prop, val in outputs.items():
                # TODO: Special cases are bad
                if name == "LoadDiffCal":
                    if prop in ["GroupingWorkspace", "MaskWorkspace", "CalWorkspace"]:
                        continue
                returnVal = getattr(algorithm.getProperty(prop), "value", None)
                if returnVal is None:
                    returnVal = getattr(algorithm.getProperty(prop), "valueAsStr", None)
                val.update(returnVal)
        except RuntimeError as e:
            raise AlgorithmException(name, str(e))

    def executeQueue(self):
        if self.parentAlgorithm:
            self._prog_reporter = Progress(self.parentAlgorithm, start=0.0, end=1.0, nreports=self._endrange)
        for algorithmTuple in self._algorithmQueue:
            if self._export:
                self._exportScript += "{}(".format(algorithmTuple[0])
                for prop, val in algorithmTuple[2].items():
                    self._exportScript += "{}={}, ".format(
                        prop, val if not isinstance(val, str) else "'{}'".format(val)
                    )
                self._exportScript = self._exportScript[:-2]
                self._exportScript += ")\n"

            self.reportAndIncrement(algorithmTuple[1])
            logger.info(algorithmTuple[1])
            # import pdb; pdb.set_trace()
            self.executeAlgorithm(name=algorithmTuple[0], outputs=algorithmTuple[3], **algorithmTuple[2])
        self.cleanup()

    @property
    def mtd(self):
        return self._mtd

    def _cleanOldExport(self):
        exportPath = self._generateExportPath()
        if os.path.exists(exportPath):
            os.remove(exportPath)

    def _generateExportPath(self):
        return Resource.getPath("{}_export.py".format(self._name))

    def cleanup(self):
        if self._export:
            exportPath = self._generateExportPath()
            with open(exportPath, "a") as file:
                file.write(self._exportScript)
        if self.parentAlgorithm:
            self._prog_reporter.report(self._endrange, "Done")
        self._progressCounter = 0
        self.algorithmQueue = []
