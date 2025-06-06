import time
from collections import namedtuple
from threading import Lock
from typing import Any, Dict

from mantid.api import AlgorithmManager, IAlgorithm, IWorkspaceProperty, Progress, mtd
from mantid.kernel import Direction
from mantid.kernel import ULongLongPropertyWithValue as PointerProperty

from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.log.logger import snapredLogger

# must import to register with AlgorithmManager
from snapred.meta.Callback import callback
from snapred.meta.Config import Config, Resource
from snapred.meta.pointer import access_pointer, create_pointer

logger = snapredLogger.getLogger(__name__)


class _CustomMtd:
    def __getitem__(self, key):
        if str(key.__class__) == str(callback(int).__class__):
            key = key.get()
        if self.doesExist(key):
            return mtd[key]
        raise KeyError(f"Workspace {key} not found in mtd: {self.getObjectNames()}")

    def doesExist(self, key):
        if str(key.__class__) == str(callback(int).__class__):
            key = key.get()
        return key is not None and mtd.doesExist(key)

    def unique_name(self, n=5, prefix="", suffix=""):
        return mtd.unique_name(n=n, prefix=prefix, suffix=suffix)

    def unique_hidden_name(self):
        return mtd.unique_hidden_name()

    def getSNAPRedLog(self, wsName, logname):
        realLogName = f"{Config['metadata.tagPrefix']}{logname}"
        try:
            return self[wsName].getRun().getLogData(realLogName).value
        except RuntimeError as e:
            if "Unknown property search object" in str(e):
                raise KeyError(f"SNAPRed log {realLogName} not found in {wsName}")
            raise

    def getObjectNames(self):
        return mtd.getObjectNames()


class MantidSnapper:
    ##
    ## KNOWN NON-REENTRANT ALGORITHMS
    ##
    _nonReentrantMutexes = {"LoadLiveData": Lock()}

    ##
    ## KNOWN NON-CONCURRENT ALGORITHMS
    ##
    _nonConcurrentAlgorithms = "SaveNexus", "SaveNexusESS", "SaveDiffCal", "RenameWorkspace"
    _nonConcurrentAlgorithmMutex = Lock()

    ##
    ## ALGORITHMS WITH LOGGING DISABLED
    ##

    # Important: completely disable logging from `GetIPTS` and `LoadLiveData`.
    # TODO:  Poco::Logger may have a race condition (, with respect to the GIL),
    #   when called from Python in async mode.
    _quietAlgorithms = "GetIPTS", "LoadLiveData"

    # Timeout sequencing
    _timeout = 60.0  # seconds
    _checkInterval = 0.05  # 50 ms

    typeTranslationTable = {"string": str, "number": float, "dbl list": list, "boolean": bool}
    _mtd = _CustomMtd()

    _infoMutex = Lock()
    _workspacesInfo: Dict[str, Dict[str, Any]] = {}

    def __init__(self, parentAlgorithm, name, non_queued_execution=False):
        """
                                        :;:::::;:
                                      ;;;;;::;;:::::::
                                     :;;;;::;;:;:::::::;:
                                   ;;,,,,,,,;;;;;;::::::::::::
                               cc:::cccccccccc::::;;:;;:::;;;;,,,,;;;,,;                             kxl
                          l:;:cllooddxxxkkkOkkxxxddooollccc::;,,,,,,,,,,;;;;::                    xol::;,
                     dlccccclodxxxxkkxkxxkkkxxkkkkkkkkkkxxxdoolc:;,,,,,,,,,,;;;;:              dlcccc:;,:
                   l;,;:clodxxkkkkxkkkkkkkkkkkkkkkkxxkkkkkkkkxxddollc:;;,,,,;;;:            dcccllcccc:,
                o:;,,;:cloodxxkkkkOOOOOOOOOOOOkkkkkkOOOOOOOkkkkkkkxxxdolcc:;:             o:;:cccllccc;,
             o::c:,..',cooooddkOOOOOOOOOOOOOOOOkOOOOOOOOOOOOOOOOOOOOkkkxxxdolclloddddddolcc::::clllccc;;
           c;;:odo;'..;dxxxkOO0K000000000000000OOOOOOOO00000OO000000OOOOOkkkxxxxdddddxxxxdolc:cccccc::,,
         :cloodxkkxoodk0OkO0KKKK000KKKKKK00OOOkkkkxxxkkxxkkk0KKKKKKKKKKKKK000000000OOOOOOkxdllcclllc:;..
        doodxkkkkkkOO00kkkO0K00K00OOOkkxdoooooooolllllodxOO0KXXXXXXXXXXXXXXKXKKKKKKKKKKK000Odlcccccc:,..
        xxdoodk0OkxxkkkxkkO0KXK00OdolccccllloooolllodkO0KXXXXXXXXXXXXXXXK0OkkxxxxxxxxxxkkOOkdlcccc:c:;';
           kdddxkxxxxxkOOOO0000KKKOdllllloooooooodk0KXXNNNXXXXXKKXXKK00kxdodxO0         xdooolccccc::;';
                kxxxxxkkkkkkk0XXXXKklccloodddddkOKKXXXXXXKXXKK00OOOkxdoollodxO             xoolcccc::;';
                     kxddoddk0000K0OdoooooddxkO00KK0000000OOkkddooolllllooodxxk              kdlcc:::;,'
                            OOkkxxxxxddddddddxxxxxxxxddxxxxxxdollllllllloddxxddk                xl:;;;,':
                                    xlllldkOOO00000            kdooooddooddxxdddk                  xo:,''
                                     xlllodk
                                      kollloO
                                       kolloO
                                        0dlo0
                                          OO
        """
        
        # `non_queued_execution`: flag to allow immediate (as opposed to deferred) execution when debugging.
        
        self.parentAlgorithm = parentAlgorithm
        self._name = name
        self._non_queued_execution = non_queued_execution
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
            mantidAlgorithm = self._createAlgorithm(key)
            # Get all `Output` properties.
            for prop in mantidAlgorithm.getProperties():
                if Direction.values[prop.direction] == Direction.Output:
                    outputProperties[prop.name] = self.createOutputCallback(prop)
            # Get any `InOut` properties that are actually referenced in the kwargs.
            for propName in set(kwargs.keys()).difference(mantidAlgorithm.getProperties()):
                prop = mantidAlgorithm.getProperty(propName)
                if Direction.values[prop.direction] == Direction.InOut:
                    outputProperties[prop.name] = self.createOutputCallback(prop)

            # TODO: Special cases are bad.
            if key == "LoadDiffCal":
                if kwargs.get("MakeGroupingWorkspace", True):
                    outputProperties["GroupingWorkspace"] = kwargs["WorkspaceName"] + "_group"
                if kwargs.get("MakeMaskWorkspace", True):
                    outputProperties["MaskWorkspace"] = kwargs["WorkspaceName"] + "_mask"
                if kwargs.get("MakeCalWorkspace", True):
                    outputProperties["CalWorkspace"] = kwargs["WorkspaceName"] + "_cal"

            # We're done with the instance for now, so remove it from managed algorithms.
            self._removeAlgorithm(mantidAlgorithm)

            self._algorithmQueue.append((key, message, kwargs, outputProperties))

            if len(outputProperties) == 1:
                # If there's only one output property, return it directly.
                (outputProperties,) = outputProperties.values()
            else:
                # For multiple outputs, convert to a tuple for a more pythonic return.
                NamedOutputsTuple = namedtuple("{}Outputs".format(key), outputProperties.keys())
                outputProperties = NamedOutputsTuple(**outputProperties)

            # debugging-mode: non-deferred execution
            if self._non_queued_execution:
                self.executeQueue()
            
            return outputProperties

        return enqueueAlgorithm

    def reportAndIncrement(self, message):
        if not self._prog_reporter:
            return
        self._prog_reporter.reportIncrement(self._progressCounter, message)
        self._progressCounter += 1

    @classmethod
    def _createAlgorithm(cls, name):
        alg = AlgorithmManager.create(name)

        alg.setChild(True)
        # *** DEBUG ***
        alg.setLogging(True)
        
        if name in cls._quietAlgorithms:
            # This allows a distinction between `setChild`, and logging completely turned off.
            alg.setLogging(False)
        alg.setAlwaysStoreInADS(True)
        alg.setRethrows(True)
        return alg

    @classmethod
    def _removeAlgorithm(cls, alg: IAlgorithm):
        # Remove a Mantid-managed algorithm.

        # TODO: this implicitly assumes that the algorithm is not running -- any cancellation
        #   sequence should be done elsewhere!
        AlgorithmManager.removeById(alg.getAlgorithmID())

    @classmethod
    def _waitForAlgorithmCompletion(cls, name):
        currentTimeout = 0
        while len(AlgorithmManager.runningInstancesOf(name)) > 0:
            if currentTimeout >= cls._timeout:
                raise TimeoutError(f"Timeout occurred while waiting for instance of {name} to cleanup")
            currentTimeout += cls._checkInterval
            time.sleep(cls._checkInterval)

    @classmethod
    def _obtainMutex(cls, name):
        mutex = cls._nonReentrantMutexes.get(name)
        if mutex is None and name in cls._nonConcurrentAlgorithms:
            mutex = cls._nonConcurrentAlgorithmMutex
        return mutex

    def executeAlgorithm(self, name, outputs, **kwargs):
        algorithm = self._createAlgorithm(name)
        mutex = None
        try:
            # Protect non-reentrant algorithms.

            mutex = self._obtainMutex(name)
            if mutex is not None:
                mutex.acquire()

            for prop, val in kwargs.items():
                # this line is to appease mantid properties, idk where its pulling empty string from
                if str(val.__class__) == str(callback(int).__class__):
                    val = val.get()
                if val is None:
                    continue
                # for pointer property, set via its pointer
                # allows for "pass-by-reference"-like behavior
                # this is safe even if the memory address is directly passed
                if isinstance(algorithm.getProperty(prop), PointerProperty) and type(val) is not int:
                    val = create_pointer(val)
                algorithm.setProperty(prop, val)
            if not algorithm.execute():
                raise RuntimeError(f"{name} failed to execute")
            for prop, val in outputs.items():
                # TODO: Special cases are bad
                if name == "LoadDiffCal":
                    if prop in ["GroupingWorkspace", "MaskWorkspace", "CalWorkspace"]:
                        suffix = {"GroupingWorkspace": "_group", "MaskWorkspace": "_mask", "CalWorkspace": "_cal"}
                        wsname = algorithm.getProperty("WorkspaceName").valueAsStr + suffix[prop]
                        MantidSnapper._addWorkspaceInfo(wsname, name, prop)
                        continue
                returnVal = getattr(algorithm.getProperty(prop), "value", None)
                if returnVal is None:
                    returnVal = getattr(algorithm.getProperty(prop), "valueAsStr", None)
                if isinstance(algorithm.getProperty(prop), PointerProperty):
                    returnVal = access_pointer(returnVal)
                val.update(returnVal)
                if isinstance(algorithm.getProperty(prop), IWorkspaceProperty) and not (
                    algorithm.getProperty(prop).isDefault and algorithm.getProperty(prop).isOptional()
                ):
                    MantidSnapper._addWorkspaceInfo(algorithm.getProperty(prop).valueAsStr, name, prop)
        except (RuntimeError, TypeError) as e:
            logger.error(f"Algorithm {name} failed for the following arguments: \n {kwargs}")
            self.cleanup()
            raise AlgorithmException(name, str(e)) from e
        finally:
            self._cleanupNonConcurrent(name, algorithm)
            if mutex is not None:
                mutex.release()

    @classmethod
    def _cleanupNonConcurrent(cls, name, algorithm):
        if name in cls._nonConcurrentAlgorithms:
            cls._waitForAlgorithmCompletion(name)
            cls._removeAlgorithm(algorithm)

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

            # TODO: in general, SNAPRed needs "headers-based" logging -- but for the moment,
            #   at least put the algorithm name into the log message!
            logger.info("%s - %s", *algorithmTuple[0:2])
            self.executeAlgorithm(name=algorithmTuple[0], outputs=algorithmTuple[3], **algorithmTuple[2])

        self.cleanup()

    @property
    def mtd(self):
        return self._mtd

    def _cleanOldExport(self):
        # exportPath = self._generateExportPath()
        # if os.path.exists(exportPath):
        #     os.remove(exportPath)
        pass

    def _generateExportPath(self):
        return Resource.getPath("snapper_export.py")

    def cleanup(self):
        if self._export:
            exportPath = self._generateExportPath()
            with open(exportPath, "a") as file:
                file.write(self._exportScript)
        if self.parentAlgorithm:
            self._prog_reporter.report(self._endrange, "Done")
        self._progressCounter = 0
        self._algorithmQueue = []

    @classmethod
    def _addWorkspaceInfo(cls, workspaceName: str, algorithmName: str, propertyName: str):
        cls._infoMutex.acquire()
        cls._workspacesInfo[workspaceName] = {
            "propertyName": propertyName,
            "algorithm": algorithmName,
        }
        cls._infoMutex.release()

    @classmethod
    def getWorkspacesInfo(cls) -> Dict[str, Dict[str, Any]]:
        """
        Grab dictionary of workspaces created by MantidSnapper

        :return: dict by workspace-name key of information about the output workspaces
        :rtype: Dict[str, Dict[str, Any]]
        """
        # WARNING: for access that isn't READ-ONLY, synchronization will be required!
        return cls._workspacesInfo
