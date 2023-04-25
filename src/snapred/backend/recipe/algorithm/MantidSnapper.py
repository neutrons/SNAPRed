import os

from mantid.api import AlgorithmManager, Progress
from mantid.kernel import Direction
from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Resource
# must import to register with AlgorithmManager
from snapred.backend.recipe.algorithm.CustomGroupWorkspace import CustomGroupWorkspace

logger = snapredLogger.getLogger(__name__)


class MantidSnapper:
    def __init__(self, parentAlgorithm, name):
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

    def __getattr__(self, key):
        def enqueueAlgorithm(message, **kwargs):
            self._algorithmQueue.append((key, message, kwargs))
            self._endrange += 1
            # inspect mantid algorithm for output properties
            # if there are any, add them to a list for return
            outputProperties = {}
            mantidAlgorithm = AlgorithmManager.create(key)
            for propname in set(kwargs.keys()).difference(mantidAlgorithm.getProperties()):
                prop = mantidAlgorithm.getProperty(propname)
                if Direction.values[prop.direction] == Direction.Output:
                    outputProperties[prop.name] = kwargs[prop.name]
                if Direction.values[prop.direction] == Direction.InOut:
                    outputProperties[prop.name] = kwargs[prop.name]

            # TODO: Special cases are bad
            if key == "LoadDiffCal":
                if kwargs.get('MakeGroupingWorkspace', True):
                    outputProperties['GroupingWorkspace'] = kwargs['WorkspaceName'] + "_group"
                if kwargs.get('MakeMaskWorkspace', True):
                    outputProperties['MaskWorkspace'] = kwargs['WorkspaceName'] + '_mask'
                if kwargs.get('MakeCalWorkspace', True):
                    outputProperties['CalWorkspace'] = kwargs['WorkspaceName'] + '_cal'

            # remove mantid algorithm from managed algorithms
            AlgorithmManager.removeById(mantidAlgorithm.getAlgorithmID())
            # if only one property is returned, return it directly
            if len(outputProperties) == 1:
                outputProperties, = outputProperties.values() 

            return outputProperties

        return enqueueAlgorithm

    def reportAndIncrement(self, message):
        self._prog_reporter.reportIncrement(self._progressCounter, message)
        self._progressCounter += 1

    def createAlgorithm(self, name):
        alg = AlgorithmManager.create(name)
        alg.setChild(True)
        alg.setAlwaysStoreInADS(True)
        alg.setRethrows(True)
        return alg

    def executeAlgorithm(self, name, **kwargs):
        algorithm = self.createAlgorithm(name)
        try:
            for prop, val in kwargs.items():
                algorithm.setProperty(prop, val)
            if not algorithm.execute():
                raise RuntimeError("")
        except RuntimeError as e:
            raise AlgorithmException(name, str(e))

    def executeQueue(self):
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
            self.executeAlgorithm(name=algorithmTuple[0], **algorithmTuple[2])
        self.cleanup()

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
        self._prog_reporter.report(self._endrange, "Done")
        self._progressCounter = 0
        self.algorithmQueue = []