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

import mock
from mantid.api import PythonAlgorithm
from qtpy.QtCore import Property, QThread, Signal

# Store original __import__
orig_import = __import__


def import_mock(name, *args):
    imports = orig_import(name, *args)
    if name == "mantid.api":
        fromlist = args[2]
        if "PythonAlgorithm" in fromlist:
            imports.PythonAlgorithm = PythonAlgorithm
    if name == "qtpy.QtCore":
        fromlist = args[2]
        if "QThread" in fromlist:
            imports.QThread = QThread
        if "Signal" in fromlist:
            imports.Signal = Signal
        if "Property" in fromlist:
            imports.Property = Property
    return imports


with mock.patch("builtins.__import__", side_effect=import_mock):
    from mantid.api import PythonAlgorithm  # noqa: F811
    from qtpy.QtCore import Property, QThread, Signal  # noqa: F811

    autodoc_mock_imports = [
        "mantid",
        "mantid.kernel",
        "mantid.utils",
        "mantid.utils.logging",
        "mantid.simpleapi",
        "mantid.geometry",
        "mantidqt.widgets",
        "mantidqt.widgets.algorithmprogress",
        "qtpy",
        "qtpy.uic",
        "qtpy.QtWidgets",
        "qtpy.QtGui",
        "qtpy.QThread",
        "mantid.plots",
        "mantid.plots.plotfunctions",
        "mantid.plots.datafunctions",
        "mantid.plots.utility",
        "workbench.plugins.workspacewidget",
    ]

    project = "SNAPRed"
    project_copyright = "2023, ORNL"
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

    html_theme = "sphinx_rtd_theme"

    # -- Options for EPUB output
    epub_show_urls = "footnote"
