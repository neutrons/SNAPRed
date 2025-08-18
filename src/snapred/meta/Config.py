import copy
import importlib.resources as resources
import logging
import os
import re
import shutil
import subprocess
import sys
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, TypeVar

import yaml
from ruamel.yaml import YAML as rYaml

from snapred import __version__ as snapredVersion
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.Enum import StrEnum
from snapred.meta.Time import isoFromTimestamp, timestamp


class DeployEnvEnum(StrEnum):
    NEXT = "snapred_next"
    QA = "snapred_qa"
    PROD = "snapred_prod"


def isTestEnv() -> bool:
    """
    Check if the current environment is a test environment.
    This is determined by the presence of the "env" environment variable
    and whether it contains the string "test".
    """
    env = os.environ.get("env")
    return env and "test" in env and "conftest" in sys.modules


def _find_root_dir():
    try:
        MODULE_ROOT = Path(sys.modules["snapred"].__file__).parent

        # Using `"test" in env` here allows different versions of "[category]_test.yml" to be used for different
        #  test categories: e.g. unit tests use "test.yml" but integration tests use "integration_test.yml".
        if isTestEnv():
            # WARNING: there are now multiple "conftest.py" at various levels in the test hierarchy.
            MODULE_ROOT = MODULE_ROOT.parent.parent / "tests"
    except Exception as e:
        raise RuntimeError("Unable to determine SNAPRed module-root directory") from e

    return str(MODULE_ROOT)


@Singleton
class _Resource:
    _packageMode: bool
    _resourcesPath: str
    _logger = logging.getLogger(__name__ + ".Resource")

    def __init__(self):
        # where the location of resources are depends on whether or not this is in package mode
        self._packageMode = not self._existsInPackage("application.yml")
        if self._packageMode:
            self._logger.debug("In package mode")
            self._resourcesPath = "/resources/"
        else:
            self._logger.debug("Not in package mode")
            self._resourcesPath = os.path.join(_find_root_dir(), "resources/")

    def _existsInPackage(self, subPath) -> bool:
        with resources.path("snapred.resources", subPath) as path:
            return os.path.exists(path)

    def exists(self, subPath) -> bool:
        if self._packageMode:
            return self._existsInPackage(subPath)
        else:
            return os.path.exists(self.getPath(subPath))

    def getPath(self, subPath):
        if subPath.startswith("/"):
            return os.path.join(self._resourcesPath, subPath[1:])
        else:
            return os.path.join(self._resourcesPath, subPath)

    def read(self, subPath):
        with self.open(subPath, "r") as file:
            return file.read()

    def open(self, subPath, mode):  # noqa: A003
        if self._packageMode:
            with resources.path("snapred.resources", subPath) as path:
                return open(path, mode)
        else:
            return open(self.getPath(subPath), mode)


Resource = _Resource()

KeyType = TypeVar("KeyType")


def deep_update(mapping: Dict[KeyType, Any], *updating_mappings: Dict[KeyType, Any]) -> Dict[KeyType, Any]:
    updated_mapping = mapping.copy()
    for updating_mapping in updating_mappings:
        for k, v in updating_mapping.items():
            if k in updated_mapping and isinstance(updated_mapping[k], dict) and isinstance(v, dict):
                updated_mapping[k] = deep_update(updated_mapping[k], v)
            else:
                updated_mapping[k] = v
    return updated_mapping


@Singleton
class _Config:
    _config: Dict[str, Any] = {}
    _logger = logging.getLogger(__name__ + ".Config")
    _propertyChangeWarnings: Dict[str, str] = {}
    _defaultEnv: str = "application.yml"

    def __init__(self):
        self._propertyChangeWarnings = {
            "version.start": (
                "It is NOT ADVISED to change the `version.start` property"
                " WITHOUT CAREFUL CONSIDERATION of the file indexes on disk."
            ),
        }

        self.reload()
        # if snapred-user.yml exists, then load it by default
        if (self._userHome() / "snapred-user.yml").exists() and not isTestEnv():
            self._logger.info("Found user configuration file, swapping to it")
            self.swapToUserYml()

    def _fix_directory_properties(self):
        """Some developers set instrument.home to use ~/ and this fixes that"""

        def expandhome(direc: str) -> str:
            if "~" in direc:
                return str(Path(direc).expanduser())
            else:
                return direc

        if "instrument" in self._config and "home" in self._config["instrument"]:
            self._config["instrument"]["home"] = expandhome(self._config["instrument"]["home"])
        if "samples" in self._config and "home" in self._config["samples"]:
            self._config["samples"]["home"] = expandhome(self._config["samples"]["home"])

    def configureForDeploy(self) -> None:
        version = self.snapredVersion()
        if "dev" in version:
            self.mergeAndExport(DeployEnvEnum.NEXT)
        elif "rc" in version:
            self.mergeAndExport(DeployEnvEnum.QA)
        else:
            self.mergeAndExport(DeployEnvEnum.PROD)

    def mergeAndExport(self, envName: str) -> None:
        """
        Merge the current configuration with the specified environment configuration
        and export it to the application.yml file.
        """
        self._logger.debug(f"Merging/exporting configuration with environment: {envName}")
        self.refresh(envName, False)

        ryaml = rYaml()
        ryaml.default_flow_style = False
        ryaml.indent(mapping=2, sequence=4, offset=2)

        with Resource.open(self._defaultEnv, "r") as file:
            config = ryaml.load(file)

        # update config with self._config, deep_update does not work with ruamel.yaml
        def merge_dicts(d1, d2):
            for key, value in d2.items():
                if isinstance(value, dict) and key in d1:
                    merge_dicts(d1[key], value)
                else:
                    d1[key] = value

        merge_dicts(config, self._config)

        # Export the merged configuration to application.yml
        with Resource.open(self._defaultEnv, "w") as file:
            ryaml.dump(config, file)

    def reload(self, env_name=None) -> None:
        # use refresh to do initial load, clearing shouldn't matter
        self.refresh(self._defaultEnv, True)

        # ---------- SNAPRed-internal values: --------------------------
        # allow "resources" relative paths to be entered into the "yml"
        #   using "${module.root}"
        self._config["module"] = {}
        self._config["module"]["root"] = _find_root_dir()

        self._config["version"] = self._config.get("version", {})
        self._config["version"]["default"] = -1
        # ---------- end: internal values: -----------------------------

        watchedProperties = {}
        for key in self._propertyChangeWarnings:
            if self.exists(key):
                watchedProperties[key] = self[key]

        # see if user used environment injection to modify what is needed
        # this will get from the os environment or from the currently loaded one
        # first case wins
        self.env = env_name
        if self.env is None:
            self.env = os.environ.get("env", self._config.get("environment", None))
        if self.env is not None:
            self._logger.info(f"Loading environment config: {self.env}")
            self.refresh(self.env)
        else:
            self._logger.info("No environment config specified, using default")
        self.warnSensitiveProperties(watchedProperties)
        self.persistBackup()

    @property
    def userApplicationDataHome(self) -> Path:
        userApplicationDataHome = Path.home() / ".snapred"
        return userApplicationDataHome

    def persistBackup(self) -> None:
        self.userApplicationDataHome.mkdir(parents=True, exist_ok=True)
        backupFile = self.userApplicationDataHome / "application.yml.bak"
        with open(backupFile, "w") as file:
            yaml.dump(self._config, file, default_flow_style=False)
        self._logger.info(f"Backup of application.yml created at {backupFile.absolute()}")

    def loadEnv(self, env_name: str) -> None:
        # load the new environment
        self.reload(env_name)

    @staticmethod
    def _timestamp():
        return isoFromTimestamp(timestamp())

    def archiveUserYml(self):
        # check if snapred-user.yml exists
        userHome = self._userHome()
        if (userHome / "snapred-user.yml").exists():
            version = self.getUserYmlVersionDisk()

            # generate human readable timestamp
            timestamp = self._timestamp()

            # archive the old snapred-user.yml
            archivePath = userHome / f"snapred-user-{version}-{timestamp}.yml.bak"
            shutil.copy(str(userHome / "snapred-user.yml"), str(archivePath))

    @staticmethod
    def _userHome():
        return Path.home() / ".snapred"

    def snapredVersion(self):
        if snapredVersion is None or snapredVersion == "unknown":
            label = subprocess.check_output(["git", "describe"]).strip()
            if label:
                return label.decode("utf-8")
        return snapredVersion

    def snapwrapVersion(self):
        try:
            import snapwrap

            return snapwrap.__version__
        except ImportError:
            return None

    def getUserYmlVersionDisk(self):
        # check if snapred-user.yml exists
        userHome = self._userHome()
        if (userHome / "snapred-user.yml").exists():
            with open(str(userHome / "snapred-user.yml"), "r") as f:
                applicationYml = yaml.safe_load(f)
            version = applicationYml.get("application", {"version": None})["version"]
            return version
        else:
            return None

    def swapToUserYml(self):
        # generate root directory for user configurations
        userHome = self._userHome()

        try:
            if not userHome.exists():
                userHome.mkdir(parents=True, exist_ok=True)

            # check if snapred-user.yml exists
            if self.getUserYmlVersionDisk() != self.snapredVersion():
                # if the version is not the same, then we need to archive the old one
                if self.getUserYmlVersionDisk() is not None:
                    self._logger.warning(
                        "The user configuration file is out of dateA new configuration file will be generated."
                    )
                # archive the old snapred-user.yml
                self.archiveUserYml()
                # generate a new valid snapred-user.yml
                self._generateUserYml()

        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                (
                    "Unable to swap to user configuration: "
                    f"{userHome / 'snapred-user.yml'}"
                    "\nOriginal configuration maintained."
                )
            ) from e
        else:
            # load the user yml file if the filesystem is ready
            self.loadEnv(str(userHome / "snapred-user.yml"))
            # archive a backup of the current user yml
            self.archiveUserYml()

    def _generateUserYml(self):
        userHome = self._userHome()
        # copy commented out application.yml as snapred-user.yml
        # read the application.yml as a dict from resources
        applicationYml = None
        with Resource.open("application.yml", "r") as f:
            applicationYml = yaml.safe_load(f)

        # overwrite the snapred-user.yml calibration home with the user specified one
        userCalibrationHome = str(userHome / "Calibration")
        applicationYml["instrument"]["calibration"]["home"] = userCalibrationHome
        if applicationYml.get("application") is None:
            applicationYml["application"] = {}
        applicationYml["application"]["version"] = self.snapredVersion()
        applicationYml["environment"] = "snapred-user"

        # convert the dict back to a yaml string
        applicationYmlStr = yaml.dump(applicationYml, default_flow_style=False)

        # write the application.yml to the user home as snapred-user.yml
        with open(userHome / "snapred-user.yml", "w") as f:
            f.write(applicationYmlStr)

    def getCurrentEnv(self) -> str:
        if self.env is not None:
            return self.env
        else:
            # this is the default environment
            return "default"

    def refresh(self, env_name: str, clearPrevious: bool = False) -> None:
        # save a copy of pervious config if it fails to load
        prevConfig = copy.deepcopy(self._config)

        try:
            if clearPrevious:
                self._config.clear()

            if env_name.endswith(".yml"):
                # name to be put into config
                new_env_name = env_name

                # this is a filename that should be loaded
                filepath = Path(env_name)
                if filepath.exists():
                    self._logger.debug(f"Loading config from {filepath.absolute()}")
                    with open(filepath, "r") as file:
                        envConfig = yaml.safe_load(file)
                else:
                    # load from the resource
                    with Resource.open(env_name, "r") as file:
                        envConfig = yaml.safe_load(file)
                    new_env_name = env_name.replace(".yml", "")
                # update the configuration with the  new environment
                self._config = deep_update(self._config, envConfig)

                # add the name to the config object if it wasn't specified
                if "environment" not in envConfig:
                    self._config["environment"] = new_env_name

                # do fixups on items that are directories
                self._fix_directory_properties()
            else:
                # recurse this function with a fuller name
                self.refresh(f"{env_name}.yml", clearPrevious)
        except Exception:
            # if it fails, restore the previous config
            self._logger.warning(f"Failed to load {env_name}.yml, restoring previous config")
            self._config = prevConfig
            raise

    def warnSensitiveProperties(self, watchedProperties) -> None:
        for key in watchedProperties:
            msg = self._propertyChangeWarnings[key]
            if watchedProperties[key] != self[key]:
                warningBar = ("/" * 20) + " WARNING " + ("/" * 20)
                self._logger.warning(warningBar)
                self._logger.warning(f"Property '{key}' was changed in {self.env}.yml")
                self._logger.warning(msg)
                self._logger.warning(warningBar)

    # method to regex for string pattern of ${key} and replace with value
    def _replace(self, value: str, remainingKeys) -> str:
        # if the value is not a string, then just return it
        if not isinstance(value, str):
            return value

        # Regex all keys of the form ${key.subkey} and store in a list
        regex = r"\$\{([a-zA-Z0-9_\.]+)\}"
        matches = [match for match in re.finditer(regex, value, re.MULTILINE)]
        # replace all keys with their values
        if len(remainingKeys) == 0:
            for match in matches:
                key = match.group()[2:-1]
                if isinstance(self[key], dict):
                    return value
                value = value.replace(f"${{{key}}}", self[key])
        else:
            match = matches[0]
            rootKey = match.group()[2:-1]
            key = rootKey
            val = self[key]

            # while val is a dict, keep appending keys
            while isinstance(val, dict):
                key = key + f".{remainingKeys.pop(0)}"
                val = self[key]

            value = value.replace(f"${{{rootKey}}}", val)
            # if(len(remainingKeys) > 0):
            value = self._replace(value, remainingKeys)

        return value

    def exists(self, key: str) -> bool:
        val = self._find(key)
        return val is not None

    def _find(self, key: str) -> Any:
        keys = key.split(".")
        val = self._config.get(keys[0])
        totalProcessed = 0
        for k in keys[1:]:
            if val is None:
                break
            if isinstance(val, str):
                break
            totalProcessed += 1
            val = val.get(k)

        if val is not None:
            val = self._replace(val, keys[1 + totalProcessed :])
        return val

    # period delimited key lookup
    def __getitem__(self, key):
        val = self._find(key)
        if val is None:
            raise KeyError(f"Key '{key}' not found in configuration")
        if isinstance(val, dict):
            # we need to solve the keys present in dict members
            # we can accomplish this by recursively calling __getitem__
            return {k: self[f"{key}.{k}"] for k in val}
        return val

    def validate(self):
        # Warn the user about any issues with `Config` settings.

        # Implementation notes:
        #
        #   * Do not prevent the user from doing something that won't outright "break" SNAPRed.
        #   Where at all possible, this method should WARN, otherwise it should throw `RuntimeError`.
        #

        # Use SNAPRed's logger for any warnings:
        from snapred.backend.log.logger import snapredLogger  # prevent circular import

        logger = snapredLogger.getLogger(__name__ + ".Config")

        if self["liveData.enabled"] and self["mantid.workspace.normalizeByBeamMonitor"]:
            logger.warning(
                "Both 'mantid.workspace.normalizeByBeamMonitor' and 'liveData.enabled'\n"
                + "  are set in your 'application.yml'.  "
                + "This type of normalization is not yet implemented for live-data mode.  "
                + "Live-data mode will not function correctly!"
            )


Config = _Config()


# Moved to `Config.py` from `IPTS_override.py` for use in `__main__.py`.
def datasearch_directories(instrumentHome: Path) -> List[str]:
    # Construct a list of datasearch directories from a base path
    suffix = Config["nexus.native.prefix"]
    suffix = suffix[0 : suffix.find(os.sep)]
    dirs = [
        str(Path(instrumentHome).joinpath(dir_)) for dir_ in glob("*IPTS*" + os.sep + suffix, root_dir=instrumentHome)
    ]
    return dirs


# `Logging.Level` is not an `Enum`.
_pythonLoggingLevelFromString = {
    "notset": logging.NOTSET,
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}

_pythonLoggingLevelFromMantid = {
    "none": logging.NOTSET,
    "fatal": logging.CRITICAL + 5,  # exists only as a POCO level
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "notice": logging.INFO + 5,  # exists only as a POCO level
    "information": logging.INFO,
    "debug": logging.DEBUG,
    "trace": logging.DEBUG - 5,  # exists only as a POCO level
}


def fromMantidLoggingLevel(level: str) -> int:
    # Python logging level from Mantid logging level

    # Python levels:
    #   logging.NOTSET: 0, logging.DEBUG: 10, logging.INFO: 20,
    #     logging:WARNING: 30, logging.ERROR: 40, logging.CRITICAL: 50

    # Poco levels (implemented as Poco::Message::Priority enum):
    # 'none': , 'fatal', 'critical', 'error', 'warning', 'notice', 'information', 'debug', 'trace'

    pythonLevel = logging.NOTSET
    try:
        pythonLevel = _pythonLoggingLevelFromMantid[level.lower()]
    except KeyError as e:
        raise RuntimeError(f"can't convert '{e}' to a Python logging level")
    return pythonLevel


def fromPythonLoggingLevel(level: int | str) -> str:
    # Mantid logging level from Python logging level
    try:
        level = level if isinstance(level, int) else _pythonLoggingLevelFromString[level.lower()]
        mantidLevel = {v: k for k, v in _pythonLoggingLevelFromMantid.items()}[level]
    except KeyError as e:
        raise RuntimeError(f"can't convert '{e}' to a Mantid logging level")
    return mantidLevel
