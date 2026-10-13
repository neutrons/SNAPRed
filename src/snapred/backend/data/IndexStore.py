"""
Where an `Indexer`'s index and versioned data are stored.

An `IndexStore` is the single owner of the reads and writes of one indexed directory (e.g. the lite-mode
calibrations of a state): the `Indexer` keeps track of versions, while its store decides where they are read from,
where they are written to, and what has to happen before the first write.

* `IndexStore`: a single directory, used for both reads and writes.
* `FailoverIndexStore`: a directory within a `CalibrationRoot` (see `CalibrationRoot`).
  Its versions are always read from a single root: indexed data is never mixed between roots.
  Writes go only to home, through the write guard; once home is populated, home shadows the read-failovers.
  * `StateIndexStore`: the calibrations or normalizations of a calibration state.
  * `InstrumentParameterStore`: the instrument parameters.

In `recoveryMode`, a missing or corrupt index, or an index referring to missing versions, is tolerated:
  the index can then be recovered from the version directories (see `Indexer.recoverIndex`).
"""

import os
import shutil
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VERSION_START
from snapred.backend.data.CalibrationRoot import CalibrationRoot, relativePath
from snapred.backend.data.IndexerType import IndexerType, indexFilename
from snapred.backend.log.logger import snapredLogger
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import parse_file_as, write_model_list_pretty

logger = snapredLogger.getLogger(__name__)


class IndexStore:
    """A single directory, used for both reads and writes."""

    def __init__(self, indexerType: IndexerType, directory: Union[str, Path], *, recoveryMode: bool = False):
        self.indexerType = IndexerType(indexerType)
        self.recoveryMode = recoveryMode
        self._directory = Path(directory)

    @property
    def indexFilename(self) -> str:
        return indexFilename(self.indexerType)

    @property
    def readDirectory(self) -> Path:
        """The directory to read the index and its versions from: this may change between reads."""
        return self._directory

    @property
    def writeDirectory(self) -> Path:
        """The directory to write the index and its versions to."""
        return self._directory

    @property
    def isFailover(self) -> bool:
        """Whether reads currently come from a read-failover root."""
        return False

    def prepareWrite(self) -> Path:
        """Make the write directory ready for a write, and return it: called before each write."""
        return self.writeDirectory


class FailoverIndexStore(IndexStore):
    """
    An indexed directory `rel` within a `CalibrationRoot`: read from a single root, written only to home.

    * `readDirectory`: the directory in the first root that contains the index, and that `isPopulated`.
      If no such root exists: the first root that contains the index; otherwise home.
      Once home is populated, reads stay pinned to home.
    * `writeDirectory`: always the directory in home, through the write guard.
    * `prepareWrite`: before the first write to home (i.e. while home has no index), `initializeHome` is called:
      e.g. so that home doesn't start with an incomplete history.
    """

    # Whether a directory whose index contains only the default version is not populated.
    ignoresDefaultVersion: bool = False

    def __init__(
        self,
        indexerType: IndexerType,
        root: CalibrationRoot,
        rel: Union[str, Path],
        *,
        recoveryMode: bool = False,
    ):
        self._root = root
        self._rel = relativePath(rel)
        self._pinnedToHome = False
        self._warnedShadowing = False
        # `readDirectory` is evaluated before every read: only re-parse an index when it changes
        self._populated: Dict[Path, Tuple[Tuple[int, int], bool]] = {}
        super().__init__(indexerType, root.home / self._rel, recoveryMode=recoveryMode)

    @property
    def root(self) -> CalibrationRoot:
        return self._root

    @property
    def rel(self) -> Path:
        return self._rel

    @property
    def homeDirectory(self) -> Path:
        return self._root.home / self._rel

    def isPopulated(self, directory: Path) -> bool:
        """
        Whether `directory`, which contains the index, has any versions of its own:
          a directory that isn't populated doesn't shadow the directories in the lower-priority roots.
        Any error reading an existing index is raised: it must never be masked by reading another root.
        """
        if not self.ignoresDefaultVersion:
            return True
        indexPath = directory / self.indexFilename
        stat = indexPath.stat()
        key = (stat.st_mtime_ns, stat.st_size)
        cached = self._populated.get(indexPath)
        if cached is not None and cached[0] == key:
            return cached[1]
        entries = parse_file_as(List[IndexEntry], indexPath)
        populated = any(entry.version != VERSION_START() for entry in entries)
        self._populated[indexPath] = (key, populated)
        return populated

    @property
    def readDirectory(self) -> Path:
        home = self.homeDirectory
        if self._pinnedToHome:
            return home
        indexed = [root / self._rel for root in self._root.roots if (root / self._rel / self.indexFilename).exists()]
        for directory in indexed:
            if self.isPopulated(directory):
                if directory == home:
                    self._pinnedToHome = True
                return directory
        # nothing is populated: e.g. read home's default version, rather than a failover's
        return indexed[0] if indexed else home

    @property
    def writeDirectory(self) -> Path:
        # `writePath` is the write guard
        return self._root.writePath(self._rel)

    @property
    def isFailover(self) -> bool:
        return self.readDirectory != self.homeDirectory

    def prepareWrite(self) -> Path:
        directory = self.writeDirectory
        if not (directory / self.indexFilename).exists():
            if self.isFailover and not self._warnedShadowing:
                self._warnedShadowing = True
                logger.warning(
                    f"Writing to '{directory}' for the first time: "
                    f"once it has versions of its own, they shadow those of the read-failover '{self.readDirectory}'"
                )
            self.initializeHome()
        return directory

    def initializeHome(self):
        """Initialize home before its first write: by default, home starts empty."""
        pass

    ## Helpers for `initializeHome` ##

    def failoverWith(self, rel: Path) -> Optional[Path]:
        """The first read-failover root that has the directory `rel`."""
        return next((failover for failover in self._root.failovers if (failover / rel).is_dir()), None)

    def writeIndex(self, entries: List[IndexEntry], rel: Path):
        # The index is written last, and atomically: until it exists, the directory isn't initialized.
        target = self._root.writePath(rel)
        temporary = target.with_name(target.name + ".tmp")
        write_model_list_pretty(entries, temporary)
        os.replace(temporary, target)


class StateIndexStore(FailoverIndexStore):
    """
    The calibrations or normalizations of the calibration state `stateRel`, in one mode.

    * Calibration: a directory whose index contains only the default version is not populated,
      so that a state that was only initialized in home doesn't shadow a read-failover's calibrations.
      A normalization has no default version: its version 0 is a real normalization.
    * Before the first write to a state that only exists in a read-failover root, the state is initialized in home
      from the failover: its grouping-schema map, and the default version of its calibration in each mode
      (`calibrationRels`). Without this, home would start with an incomplete state: e.g. its first calibration would
      be numbered as the default version. The failover's other versions are not copied: they remain visible until
      home has its own first non-default calibration (`LocalDataService.copyCalibration` can import them).
      A new state, which isn't in any read-failover, is initialized as usual (`LocalDataService.initializeState`).
    """

    GROUPING_MAP = "groupingMap.json"

    def __init__(
        self,
        indexerType: IndexerType,
        root: CalibrationRoot,
        rel: Union[str, Path],
        *,
        stateRel: Union[str, Path],
        calibrationRels: Iterable[Union[str, Path]],
        recoveryMode: bool = False,
    ):
        super().__init__(indexerType, root, rel, recoveryMode=recoveryMode)
        self._stateRel = relativePath(stateRel)
        self._calibrationRels = [relativePath(rel) for rel in calibrationRels]

    @property
    def ignoresDefaultVersion(self) -> bool:
        return self.indexerType == IndexerType.CALIBRATION

    def initializeHome(self):
        failover = self.failoverWith(self._stateRel)
        if failover is None:
            return
        home = self.root.home

        groupingMapRel = self._stateRel / self.GROUPING_MAP
        if (failover / groupingMapRel).exists() and not (home / groupingMapRel).exists():
            target = self.root.writePath(groupingMapRel)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(failover / groupingMapRel, target)

        calibrationIndex = indexFilename(IndexerType.CALIBRATION)
        for calibrationRel in self._calibrationRels:
            sourceIndex = failover / calibrationRel / calibrationIndex
            if (home / calibrationRel / calibrationIndex).exists() or not sourceIndex.exists():
                continue
            entries = [
                entry for entry in parse_file_as(List[IndexEntry], sourceIndex) if entry.version == VERSION_START()
            ]
            if not entries:
                continue
            versionRel = calibrationRel / wnvf.pathVersion(VERSION_START())
            shutil.copytree(failover / versionRel, self.root.writePath(versionRel), dirs_exist_ok=True)
            self.writeIndex(entries, calibrationRel / calibrationIndex)
        logger.info(f"Initialized the state '{self._stateRel}' in '{home}' from the read-failover '{failover}'.")


class InstrumentParameterStore(FailoverIndexStore):
    """
    The instrument parameters.

    Their version 0 is their first epoch, not a default. Before their first write to home, all of the
      instrument parameters are copied from the read-failover: otherwise, home's single new epoch would shadow
      every one of the failover's epochs.
    """

    def __init__(self, root: CalibrationRoot, rel: Union[str, Path], *, recoveryMode: bool = False):
        super().__init__(IndexerType.INSTRUMENT_PARAMETER, root, rel, recoveryMode=recoveryMode)

    def initializeHome(self):
        failover = self.failoverWith(self.rel)
        if failover is None or not (failover / self.rel / self.indexFilename).exists():
            return
        entries = parse_file_as(List[IndexEntry], failover / self.rel / self.indexFilename)
        shutil.copytree(
            failover / self.rel,
            self.writeDirectory,
            dirs_exist_ok=True,
            ignore=lambda _, names: [name for name in names if name == self.indexFilename or name.startswith(".")],
        )
        self.writeIndex(entries, self.rel / self.indexFilename)
        logger.info(f"Initialized the instrument parameters in '{self.root.home}' from the read-failover '{failover}'.")
