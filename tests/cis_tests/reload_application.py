# import mantid algorithms, numpy and matplotlib
# from mantid.simpleapi import *
# import matplotlib.pyplot as plt
# import numpy as np
# 
# import sys
# import inspect
# import importlib
# 
# def is_config_imported(member):
#     return inspect.ismodule(member) and member.__name__ != '__main__'
#     
# 
# loaded_modules = sys.modules.keys()
# snapredModules = [module for module in loaded_modules if "snapred" in module]
# print("Currently loaded modules:")
# for module_name in snapredModules:
#     imported_modules = [name for name, member in inspect.getmembers(sys.modules[module_name])]
#     if "Config" in imported_modules:
#         print(f"{module_name} : {imported_modules}")
#         importlib.reload(sys.modules[module_name])

from snapred.backend.dao import InstrumentConfig
from snapred.backend.dao import reload
reload()


def eg(): 
    return InstrumentConfig(facility="..",
    name="..",
    nexusFileExtension="..",
    nexusFilePrefix="..",
    calibrationFileExtension="..",
    calibrationFilePrefix="..",
    calibrationDirectory="..",
    pixelGroupingDirectory="..",
    sharedDirectory="..",
    nexusDirectory="..",
    reducedDataDirectory="..",
    reductionRecordDirectory="..",
    bandwidth=1.1,
    maxBandwidth=1.1,
    L1=1.1,
    L2=1.1,
    delTOverT=1.1,
    delLOverL=1.1,
    delThNoGuide=1.1,
    delThWithGuide=1.1,
    width=1.1,
    frequency=1.1,
    version=1)


import sys
modulenames = set(sys.modules) & set(locals())
allmodules = [sys.modules[name] for name in modulenames]
print(sys.modules)
print(locals())
import inspect
clazzes = [clazz for k, clazz in locals().items() if inspect.isclass(clazz)]

import importlib
for clazz in clazzes:
    modulePath = getattr(inspect.getmodule(clazz), "__name__", None)
    # print(clazz.__name__)
    if modulePath:
        module = importlib.import_module(modulePath)
        importlib.reload(module)
        module = importlib.import_module(modulePath)
        localz = { clazz.__name__: getattr(module, clazz.__name__)}
        locals().update(localz)
        print(f"updated: {localz}")
        
    
    # importlib.reload(inspect.getmodule(clazz).__name__)
print(eg().lowWavelengthCrop)
    