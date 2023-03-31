from snapred.meta.Singleton import Singleton

from pydantic.utils import deep_update

import yaml
import os
import sys

ROOT_DIR = os.path.dirname(sys.modules['__main__'].__file__)

@Singleton
class _Resource:
    _resourcesPath = ROOT_DIR + '/snapred/resources/'
    
    def getPath(self, subPath):
        return self._resourcesPath + subPath
    
    def open(self, subPath, mode):
        return open(self.getPath(subPath), mode)
Resource = _Resource()

@Singleton
class _Config:
    _config = {}
    def __init__(self):
        baseConfigFile = Resource.getPath('application.yml')
        with open(baseConfigFile, 'r') as file:
             self._config = yaml.safe_load(file)
             # TODO: Read env from Environment Variable, not yml
             env = self._config.get("environment", None)
             if(env != None):
                envConfigFile = Resource.getPath('{}.yml'.format(env))
                with open(envConfigFile, 'r') as file:
                    envConfig = yaml.safe_load(file)
                    self._config = deep_update(self._config, envConfig)

    # period delimited key lookup
    def __getitem__(self, key):
        keys = key.split('.')
        val = self._config[keys[0]]
        for k in keys[1:]:
            if val == None: break
            val = val[k]
        return val
Config = _Config()
