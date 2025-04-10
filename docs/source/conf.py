# Configuration file for the Sphinx documentation builder.

# -- Project information

html_static_path = ["_static"]

import os

import erdantic as erd
import versioningit

# add in relative path for readthedocs
try:
    import snapred  # noqa: F401
except ImportError:
    import sys

    sys.path.insert(0, os.path.abspath("../../src"))

import unittest.mock as mock

from mantid.api import PythonAlgorithm
from qtpy.QtCore import Property, QThread, Signal  # type: ignore

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
        "workbench.plotting",
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
        "sphinxcontrib.mermaid",
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

    # from snapred.backend.dao.ingredients import ReductionIngredients
    from snapred.backend.dao import *
    from snapred.backend.dao import __all__ as daoAll
    from snapred.backend.dao.calibration import *
    from snapred.backend.dao.calibration import __all__ as calibrationAll
    from snapred.backend.dao.ingredients import *
    from snapred.backend.dao.ingredients import __all__ as ingredientsAll
    from snapred.backend.dao.normalization import *
    from snapred.backend.dao.normalization import __all__ as normalizationAll
    from snapred.backend.dao.request import *
    from snapred.backend.dao.request import __all__ as requestAll
    from snapred.backend.dao.response import *
    from snapred.backend.dao.response import __all__ as responseAll
    from snapred.backend.dao.state import *
    from snapred.backend.dao.state import __all__ as stateAll
    from snapred.backend.dao.state.CalibrantSample import *
    from snapred.backend.dao.state.CalibrantSample import __all__ as calibrantSampleAll

    daoClasses = []
    daoClasses.extend(daoAll)
    daoClasses.extend(calibrationAll)
    daoClasses.extend(normalizationAll)
    daoClasses.extend(ingredientsAll)
    daoClasses.extend(requestAll)
    daoClasses.extend(responseAll)
    daoClasses.extend(stateAll)
    daoClasses.extend(calibrantSampleAll)

    # create dao.rst file to house the toctree for all the dao classes
    daoRstPath = "./developer/dao.rst"
    # delete if it already exists
    if os.path.exists(daoRstPath):
        os.remove(daoRstPath)

    with open(daoRstPath, "w") as daoRstFile:
        daoRstFile.write("Data Access Objects\n")
        daoRstFile.write(f"{'=' * len('Data Access Objects')}\n")
        daoRstFile.write(".. toctree::\n")
        daoRstFile.write("   :maxdepth: 1\n")
        daoRstFile.write("   :caption: Data Access Objects\n")
        daoRstFile.write("\n")

    def generateDataDiagram(clazz):
        # get package path for Calibration
        root = "./developer/"
        clazzPath = root + "/".join(clazz.__module__.split("."))
        # create path if it doesn't exist
        if not os.path.exists(clazzPath):
            os.makedirs(clazzPath)

        clazzPath = clazzPath + f"/{clazz.__name__}.svg"

        erd.draw(clazz, out=clazzPath)
        # generate complimentary .rst file to import the image
        clazzRstPath = clazzPath.replace(".svg", ".rst")
        # if it exists, delete it
        if os.path.exists(clazzRstPath):
            os.remove(clazzRstPath)

        with open(clazzRstPath, "w") as clazzRstFile:
            # add title
            clazzRstFile.write(f"{clazz.__name__}\n")
            clazzRstFile.write(f"{'=' * len(clazz.__name__)}\n")
            clazzRstFile.write(f".. figure:: {clazz.__name__}.svg\n")
            clazzRstFile.write("   :align: center\n")
            clazzRstFile.write(f"   :alt: {clazz.__name__}\n")
            clazzRstFile.write("   :width: 100%\n")
            clazzRstFile.write(f"   :target: {clazz.__name__}.svg\n")
            clazzRstFile.write("\n")

        clazzRstPath = clazzRstPath.replace(".rst", "")
        clazzRstPath = clazzRstPath.replace(root, "")
        # append to dao.rst file
        with open(daoRstPath, "a") as daoRstFile:
            daoRstFile.write(f"   {clazzRstPath}\n")

    for clazz in daoClasses:
        if isinstance(clazz, str):
            clazz = eval(clazz)
        generateDataDiagram(clazz)
