import importlib.resources as resources
import logging
import os
import re
import sys
from glob import glob
from pathlib import Path
from typing import Any, Dict, List, TypeVar

import yaml

from snapred.meta.decorators.Singleton import Singleton


def _find_root_dir():
    ROOT_MODULE = None

    # Using `"test" in env` here allows different versions of "[category]_test.yml" to be used for different
    #  test categories: e.g. unit tests use "test.yml" but integration tests use "integration_test.yml".
    env = os.environ.get("env")
    if env and "test" in env and "conftest" in sys.modules:
        ROOT_MODULE = sys.modules["conftest"].__file__
    elif "snapred" in sys.modules:
        ROOT_MODULE = sys.modules["snapred"].__file__

    if ROOT_MODULE is None:
        raise Exception("Unable to determine root directory")

    return os.path.dirname(ROOT_MODULE)


@Singleton
class _Resource:
    _packageMode: bool
    _resourcesPath: str
    _logger = logging.getLogger("snapred.meta.Config.Resource")

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
    _logger = logging.getLogger("snapred.meta.Config.Config")

    def __init__(self):
        # use refresh to do initial load, clearing shouldn't matter
        self.refresh("application.yml", True)

        # allow "resources" relative paths to be entered into the "yml"
        #   using "${module.root}"
        self._config["module"] = {}
        self._config["module"]["root"] = _find_root_dir()

        # see if user used environment injection to modify what is needed
        # this will get from the os environment or from the currently loaded one
        # first case wins
        env = os.environ.get("env", self._config.get("environment", None))
        if env is not None:
            self.refresh(env)

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

    def refresh(self, env_name: str, clearPrevious: bool = False) -> None:
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

    # period delimited key lookup
    def __getitem__(self, key):
        keys = key.split(".")
        val = self._config[keys[0]]
        totalProcessed = 0
        for k in keys[1:]:
            if val is None:
                break
            if isinstance(val, str):
                break
            totalProcessed += 1
            val = val[k]

        if val is not None:
            val = self._replace(val, keys[1 + totalProcessed :])
        return val


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
