from snapred.meta.Singleton import Singleton

from pydantic.utils import deep_update

from typing import Dict, Any

import yaml
import os
import sys

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
    _resourcesPath = ROOT_DIR + "/resources/"

    def getPath(self, subPath):
        return self._resourcesPath + subPath

    def open(self, subPath, mode):
        return open(self.getPath(subPath), mode)


Resource = _Resource()


@Singleton
class _Config:
    _config: Dict[str, Any] = {}

    def __init__(self):
        baseConfigFile = Resource.getPath("application.yml")
        with open(baseConfigFile, "r") as file:
            self._config = yaml.safe_load(file)
            env = os.environ.get("env")
            if env is None:
                env = self._config.get("environment", None)
            if env is not None:
                envConfigFile = Resource.getPath("{}.yml".format(env))
                with open(envConfigFile, "r") as file:
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
