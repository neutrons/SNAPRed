# Configuration file for the Sphinx documentation builder.

# -- Project information

import versioningit

# add in relative path for readthedocs
try:
    import snapred  # noqa: F401
except ImportError:
    import os
    import sys

    sys.path.insert(0, os.path.abspath("../../src"))

from snapred.ui.main import SNAPRedGUI

project = "SNAPRed"
project_copyright = "2021, ORNL"
author = "ORNL"

# The short X.Y version
# NOTE: need to specify the location of the pyproject.toml file instead of the
#       location of the source tree
version = versioningit.get_version("../../")
# The full version, including alpha/beta/rc tags
release = ".".join(version.split(".")[:-1])

# -- General configuration

extensions = [
    "sphinx.ext.duration",
    "sphinx.ext.doctest",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

templates_path = ["_templates"]

# -- Options for HTML output

# html_theme = "sphinx_rtd_theme"

# -- Options for EPUB output
# epub_show_urls = "footnote"

# manually alter the config to point to the test resources
from mantid.kernel import ConfigService  # noqa: E402
from snapred.meta.Config import (  # noqa: E402
    Config,  # noqa: E402
    Resource,  # noqa: E402
)

Config._config["instrument"]["home"] = Resource.getPath(Config["instrument.home"])
mantidConfig = config = ConfigService.Instance()
mantidConfig["CheckMantidVersion.OnStartup"] = "0"
mantidConfig["UpdateInstrumentDefinitions.OnStartup"] = "0"
mantidConfig["usagereports.enabled"] = "0"
