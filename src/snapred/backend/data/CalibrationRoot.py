"""
Read-only failover for the calibration home.

`CalibrationRoot` overlays the configured calibration home with zero or more read-failover roots
(testing only, e.g. a QA deployment reading prod calibration data):

* reads resolve top-down: home first, then each valid failover root in priority order;
* a root is skipped only when the target is *missing*: errors reading an existing file are never masked;
* writes only ever go to home, through `writePath`, which is the single write guard.

Indexed data (an index together with its versioned records) must never be split across roots:
indexed directories are accessed through a `FailoverIndexStore` (see `IndexStore`), which reads from a single root.
"""

from pathlib import Path
from typing import Iterable, List, Optional, Union

from pydantic import ValidationError

from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationEnvironment, CalibrationHomeMarker
from snapred.backend.error.CalibrationHomeError import CalibrationHomeError
from snapred.backend.error.CalibrationWriteGuardError import CalibrationWriteGuardError
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config, isTestEnv
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.redantic import parse_file_as

logger = snapredLogger.getLogger(__name__)


def packageVersion() -> str:
    # The single place that reads the installed version:
    #   `Config.snapredVersion` becomes `packageVersion` in `neutrons_standard.config`.
    return Config.snapredVersion()


def deployEnvironment() -> CalibrationEnvironment:
    """
    The deploy environment, as determined from the *installed version*, not from any yml.

    This follows the same rule as `Config.configureForDeploy`, except that "exp" is checked first:
      exp versions may also contain "dev", and must never be classified as either next or prod.
    """
    version = packageVersion()
    if "exp" in version:
        return CalibrationEnvironment.EXP
    if "dev" in version:
        return CalibrationEnvironment.NEXT
    if "rc" in version:
        return CalibrationEnvironment.QA
    return CalibrationEnvironment.PROD


def isUserMode() -> bool:
    # A generated "snapred-user.yml" sets the top-level `environment` to "snapred-user".
    #   Note: "instrument.calibration.environment" is copied from the deployed config into user mode,
    #   so it can't be used to detect user mode.
    return str(Config["environment"]).endswith("-user")


def readFailoverPaths() -> List[Path]:
    """
    The configured read-failover roots, in priority order, after applying the prod guard.

    Read order (there are no defaults for missing keys):
      1. user mode: failover is off, and no "readFailover" key is read at all;
      2. "readFailover.enabled": if `False`, stop;
         if `True`, failover is refused for a prod deployment, or for a prod calibration environment;
      3. only then, "readFailover.paths".
    """
    if isUserMode():
        return []

    enabled = Config["instrument.calibration.readFailover.enabled"]
    if not isinstance(enabled, bool):
        raise CalibrationHomeError(
            f"'instrument.calibration.readFailover.enabled' must be a boolean, not {enabled!r} ({type(enabled)})"
        )
    if not enabled:
        return []

    if Config["instrument.calibration.environment"] == CalibrationEnvironment.PROD:
        raise CalibrationHomeError(
            "Read failover is testing only: it cannot be enabled for the 'prod' calibration environment."
        )
    if not isTestEnv() and deployEnvironment() == CalibrationEnvironment.PROD:
        raise CalibrationHomeError(
            f"Read failover is testing only: it cannot be enabled in a prod deployment (version {packageVersion()})."
        )

    # a mapping of name -> path: unlike list elements, a mapping's members are interpolated by `Config`
    paths = Config["instrument.calibration.readFailover.paths"]
    if not isinstance(paths, dict):
        raise CalibrationHomeError(
            f"'instrument.calibration.readFailover.paths' must be a mapping of name -> path, not {paths!r}"
        )
    return [Path(path) for path in paths.values()]


def relativePath(rel: Union[str, Path]) -> Path:
    rel = Path(rel)
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"expecting a path relative to the calibration root, not '{rel}'")
    return rel


class CalibrationRoot:
    """
    Overlay of `[home, *validFailovers]`.  Reads resolve top-down; writes go only to home.

    Construction validates the marker files:

      | root     | schemaVersion mismatch | environment mismatch | marker missing                       |
      |----------|------------------------|----------------------|--------------------------------------|
      | home     | error                  | error                | error if `requireMarker`, else warn  |
      | failover | warn, skip root        | allowed              | warn, skip root                      |

    In user mode, the home marker is not checked, but the write guard still applies.
    """

    def __init__(
        self,
        home: Union[str, Path],
        failovers: Iterable[Union[str, Path]] = (),
        *,
        environment: Union[str, CalibrationEnvironment],
        schemaVersion: int,
        requireMarker: bool = True,
        userMode: bool = False,
    ):
        self._home = Path(home)
        self._environment = CalibrationEnvironment(environment)
        self._schemaVersion = schemaVersion
        self._userMode = userMode

        self._homeMarker = self._validateHome(requireMarker)
        self._failovers = self._validateFailovers([Path(failover) for failover in failovers])

    @classmethod
    def fromConfig(cls) -> "CalibrationRoot":
        return cls(
            Config["instrument.calibration.home"],
            readFailoverPaths(),
            environment=Config["instrument.calibration.environment"],
            schemaVersion=Config["instrument.calibration.schemaVersion"],
            requireMarker=Config["instrument.calibration.requireMarker"],
            userMode=isUserMode(),
        )

    @property
    def home(self) -> Path:
        return self._home

    @property
    def failovers(self) -> List[Path]:
        # the *valid* failover roots, in priority order
        return list(self._failovers)

    @property
    def roots(self) -> List[Path]:
        return [self._home, *self._failovers]

    @property
    def environment(self) -> CalibrationEnvironment:
        return self._environment

    ##
    ## Validation
    ##

    @staticmethod
    def markerPath(root: Path) -> Path:
        return Path(root) / CalibrationHomeMarker.FILENAME

    @classmethod
    def readMarker(cls, root: Path) -> Optional[CalibrationHomeMarker]:
        # Returns `None` if there is no marker: any error reading an existing marker is raised.
        path = cls.markerPath(root)
        if not path.exists():
            return None
        return parse_file_as(CalibrationHomeMarker, path)

    def _validateHome(self, requireMarker: bool) -> Optional[CalibrationHomeMarker]:
        try:
            marker = self.readMarker(self._home)
        except (ValidationError, ValueError, OSError) as e:
            raise CalibrationHomeError(
                f"Unable to read the calibration-home marker '{self.markerPath(self._home)}': {e}"
            ) from e

        if self._userMode:
            # User homes are exempt from the marker check: the marker (if any) is still used by the write guard.
            return marker

        if marker is None:
            msg = (
                f"The calibration home '{self._home}' has no marker file '{CalibrationHomeMarker.FILENAME}': "
                f"it must be stamped for the '{self._environment}' environment."
            )
            if requireMarker:
                raise CalibrationHomeError(msg)
            logger.warning(msg)
            return None

        if marker.schemaVersion != self._schemaVersion:
            raise CalibrationHomeError(
                f"The calibration home '{self._home}' has schema version {marker.schemaVersion}, "
                f"but this version of SNAPRed requires schema version {self._schemaVersion}."
            )
        if marker.environment != self._environment:
            raise CalibrationHomeError(
                f"The calibration home '{self._home}' is stamped for the '{marker.environment}' environment, "
                f"but this deployment is configured for the '{self._environment}' environment."
            )
        return marker

    def _validateFailovers(self, failovers: List[Path]) -> List[Path]:
        home = self._home.resolve()
        valid = []
        resolvedValid = []
        for failover in failovers:
            resolved = failover.resolve()
            if resolved == home or resolved.is_relative_to(home) or home.is_relative_to(resolved):
                raise CalibrationHomeError(
                    f"The read-failover root '{failover}' must not be, contain, or be contained by "
                    f"the calibration home '{self._home}'."
                )
            if resolved in resolvedValid:
                logger.warning(f"Ignoring duplicate read-failover root '{failover}'.")
                continue

            try:
                marker = self.readMarker(failover)
            except (ValidationError, ValueError, OSError) as e:
                logger.warning(f"Skipping read-failover root '{failover}': unable to read its marker file: {e}")
                continue
            if marker is None:
                logger.warning(
                    f"Skipping read-failover root '{failover}': "
                    f"it has no marker file '{CalibrationHomeMarker.FILENAME}'."
                )
                continue
            if marker.schemaVersion != self._schemaVersion:
                logger.warning(
                    f"Skipping read-failover root '{failover}': it has schema version {marker.schemaVersion}, "
                    f"but schema version {self._schemaVersion} is required."
                )
                continue

            logger.info(f"Using read-failover root '{failover}' ('{marker.environment}' environment).")
            valid.append(failover)
            resolvedValid.append(resolved)
        return valid

    ##
    ## Reads
    ##

    def resolve(self, rel: Union[str, Path]) -> Path:
        """The path of `rel` in the first root where it exists."""
        rel = relativePath(rel)
        for root in self.roots:
            path = root / rel
            if path.exists():
                return path
        raise FileNotFoundError(f"'{rel}' was not found in any calibration root: {[str(root) for root in self.roots]}")

    def exists(self, rel: Union[str, Path]) -> bool:
        rel = relativePath(rel)
        return any((root / rel).exists() for root in self.roots)

    def glob(self, pattern: str) -> List[Path]:
        """The union of the matches in every root: on a relative-path collision, the higher-priority root wins."""
        relativePath(pattern)
        seen = set()
        matches = []
        for root in self.roots:
            for path in sorted(root.glob(pattern)):
                rel = path.relative_to(root)
                if rel not in seen:
                    seen.add(rel)
                    matches.append(path)
        return matches

    def listDirs(self, rel: Union[str, Path]) -> List[str]:
        """The sorted union of the names of the subdirectories of `rel`, in every root."""
        rel = relativePath(rel)
        names = set()
        for root in self.roots:
            path = root / rel
            if path.is_dir():
                names.update(child.name for child in path.iterdir() if child.is_dir())
        return sorted(names)

    ##
    ## Writes
    ##

    def writePath(self, rel: Union[str, Path]) -> Path:
        """
        The path to write `rel` to: always under home.

        This is the single write guard: it refuses to return a path that resolves outside of home
        (and so under any read-failover root), or under a home that is stamped for another environment.
        """
        rel = relativePath(rel)
        if self._homeMarker is not None and self._homeMarker.environment != self._environment:
            raise CalibrationWriteGuardError(
                f"Refusing to write to the calibration home '{self._home}': it is stamped for the "
                f"'{self._homeMarker.environment}' environment, but this deployment is configured for "
                f"the '{self._environment}' environment."
            )
        path = self._home / rel
        resolved = path.resolve()
        if not resolved.is_relative_to(self._home.resolve()):
            raise CalibrationWriteGuardError(
                f"Refusing to write '{path}': it resolves to '{resolved}', outside of the calibration home."
            )
        # No separate check against the failover roots is needed: construction guarantees that no failover root
        #   (including any skipped as invalid) is, contains, or is contained by home.
        return path


##
## The configured calibration root
##


@Singleton
class ConfiguredCalibrationRoot:
    """
    Holds the `CalibrationRoot` for the current configuration, shared by every reader and writer of calibration data.

    The root is rebuilt whenever the configuration it depends on changes (e.g. after `Config.swapToUserYml`, or a
    `Config_override` in tests), or when the home's marker file changes: the prod guard is applied at every access.
    """

    def __init__(self):
        self._key: Optional[tuple] = None
        self._root: Optional[CalibrationRoot] = None

    @staticmethod
    def _configurationKey() -> tuple:
        home = Path(Config["instrument.calibration.home"])
        markerPath = CalibrationRoot.markerPath(home)
        try:
            markerStat = markerPath.stat()
            marker = (markerStat.st_mtime_ns, markerStat.st_size)
        except FileNotFoundError:
            marker = None
        return (
            home,
            marker,
            tuple(readFailoverPaths()),
            Config["instrument.calibration.environment"],
            Config["instrument.calibration.schemaVersion"],
            Config["instrument.calibration.requireMarker"],
            isUserMode(),
        )

    def root(self) -> CalibrationRoot:
        key = self._configurationKey()
        if self._root is None or self._key != key:
            self._root = CalibrationRoot.fromConfig()
            self._key = key
        return self._root


def calibrationRoot() -> CalibrationRoot:
    """The `CalibrationRoot` for the current configuration: see `ConfiguredCalibrationRoot`."""
    return ConfiguredCalibrationRoot().root()


def calibrationRelative(path: Union[str, Path]) -> Optional[Path]:
    """
    The path of `path` relative to the calibration home, or `None` if it is not under the home.

    Calibration paths are configured as absolute paths in the home layer (e.g. "instrument.calibration.powder.home"):
      this maps them back to a path within the calibration root.  A path that is configured outside of the home
      (e.g. overridden in a test) is used as-is, without any read failover.
    """
    path = Path(path)
    home = calibrationRoot().home
    if not path.is_absolute() or not path.is_relative_to(home):
        return None
    return path.relative_to(home)


def resolveCalibrationPath(path: Union[str, Path]) -> Path:
    """
    Where to read the home-layer path `path` from: the path in the first calibration root where it exists.

    A path that is missing from every root, or that is not under the home, is returned unchanged:
      callers keep reporting missing files at their home location.
    """
    path = Path(path)
    rel = calibrationRelative(path)
    if rel is None:
        return path
    root = calibrationRoot()
    return root.resolve(rel) if root.exists(rel) else path
