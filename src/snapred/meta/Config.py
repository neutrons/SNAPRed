import importlib.resources as resources
import os
import sys
from typing import Any, Dict

import yaml
from pydantic.utils import deep_update

from snapred.meta.decorators.Singleton import Singleton

ROOT_MODULE = None
if os.environ.get("env") != "test":
    ROOT_MODULE = sys.modules["__main__"].__file__
else:
    ROOT_MODULE = sys.modules["conftest"].__file__

if ROOT_MODULE is None:
    raise Exception("Unable to determine root directory")

ROOT_DIR = os.path.dirname(ROOT_MODULE)


@Singleton
class _Resource:
    _packageMode = False
    _resourcesPath = ROOT_DIR + "/resources/"

    def __init__(self):
        try:
            self.open("application.yml", "r")
        except FileNotFoundError:
            self._packageMode = True
            self._resourcesPath = "/resources/"
        # filename = resource_filename(Requirement.parse("MyProject"),"sample.conf")

    def getPath(self, subPath):
        return self._resourcesPath + subPath

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
        with Resource.open("application.yml", "r") as file:
            self._config = yaml.safe_load(file)
            env = os.environ.get("env")
            if env is None:
                env = self._config.get("environment", None)
            if env is not None:
                with Resource.open("{}.yml".format(env), "r") as file:
                    envConfig = yaml.safe_load(file)
                    self._config = deep_update(self._config, envConfig)
            self._config["environment"] = env

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
