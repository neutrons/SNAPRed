from snapred.meta.Singleton import Singleton

from pydantic.utils import deep_update

import yaml
import os
import sys

ROOT_DIR = os.path.dirname(sys.modules['__main__'].__file__)


@Singleton
class _Config:
    _resourcesPath = ROOT_DIR + '/snapred/resources/'
    _config = {}
    def __init__(self):
        baseConfigFile = self._resourcesPath + 'application.yml'
        with open(baseConfigFile, 'r') as file:
             self._config = yaml.safe_load(file)
             # TODO: Read env from Environment Variable, not yml
             env = self._config.get("environment", None)
             if(env != None):
                envConfigFile = self._resourcesPath + '{}.yml'.format(env)
                with open(envConfigFile, 'r') as file:
                    envConfig = yaml.safe_load(file)
                    self._config = deep_update(self._config, envConfig)

    # period delimited key lookup
    def __getitem__(self, key):
        keys = key.split('.')
        val = self._config[keys[0]]
        for k in keys[1:]:
            val = val[k]
        return val

Config = _Config()