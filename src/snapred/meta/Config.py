import importlib.resources as resources
import logging
import os
import sys
from typing import Any, Dict

import yaml
from pydantic.utils import deep_update

from snapred.meta.decorators.Singleton import Singleton


def _find_root_dir():
    ROOT_MODULE = None
    if os.environ.get("env") != "test":
        ROOT_MODULE = sys.modules["__main__"].__file__
    else:
        ROOT_MODULE = sys.modules["conftest"].__file__

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


@Singleton
class _Config:
    _config: Dict[str, Any] = {}

    def __init__(self):
        # use refresh to do initial load, clearing shouldn't matter
        self.refresh("application", True)
        del self._config["environment"]

        # see if user used environment injection to modify what is needed
        # this will get from the os environment or from the currently loaded one
        # first case wins
        env = os.environ.get("env", self._config.get("environment", None))
        if env is not None:
            self.refresh(env)

    def refresh(self, env_name: str, clearPrevious: bool = False) -> None:
        if clearPrevious:
            self._config.clear()

        with Resource.open(f"{env_name}.yml", "r") as file:
            envConfig = yaml.safe_load(file)
            self._config = deep_update(self._config, envConfig)

        # add the name to the config object
        self._config["environment"] = env_name

    # period delimited key lookup
    def __getitem__(self, key):
        keys = key.split(".")
        val = self._config[keys[0]]
        for k in keys[1:]:
            if val is None:
                break
            val = val[k]
        return val


Config = _Config()
