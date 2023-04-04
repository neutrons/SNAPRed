import os

# import sys
# sys.path.append('.')
# os.environ['PYTHONPATH'] = './src'
os.environ["env"] = "test"

from snapred.meta.Config import Config  # noqa: E402
from snapred.meta.Config import Resource  # noqa: E402

# manually alter the config to point to the test resources
Config._config["instrument"]["home"] = Resource.getPath(Config["instrument.home"])
