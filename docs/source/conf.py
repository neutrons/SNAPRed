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

# Create mock classes for mantid and qtpy before any imports
class MockPythonAlgorithm:
    """Mock class for mantid.api.PythonAlgorithm"""
    def __init__(self):
        pass

    @classmethod
    def register(cls):
        """Mock register method to prevent algorithm registration errors"""
        pass

    def PyInit(self):
        """Mock PyInit method"""
        pass

    def PyExec(self):
        """Mock PyExec method"""
        pass

    def initialize(self):
        """Mock initialize method"""
        pass

    def execute(self):
        """Mock execute method"""
        pass

    def setProperty(self, name, value):
        """Mock setProperty method"""
        pass

    def getProperty(self, name):
        """Mock getProperty method"""
        return mock.MagicMock()

class MockQThread:
    """Mock class for qtpy.QtCore.QThread"""
    pass

class MockSignal:
    """Mock class for qtpy.QtCore.Signal"""
    def __init__(self, *args, **kwargs):
        # Accept any arguments to handle Signal(str, str), Signal(bool), etc.
        pass

class MockProperty:
    """Mock class for qtpy.QtCore.Property"""
    def __init__(self, *args, **kwargs):
        # Accept any arguments to handle Property decorators
        pass

    def __call__(self, func):
        # Act as a decorator and add setter attribute
        func.setter = lambda f: f  # Mock setter method
        return func

# Create mock types for common types that cause pydantic warnings
class MockBool:
    """Mock boolean type"""
    def __init__(self, value=False):
        self.value = value
    def __bool__(self):
        return self.value
    def __ror__(self, other):
        return self

class MockCrystalStructure:
    """Mock CrystalStructure type"""
    def __init__(self):
        pass
    def __ror__(self, other):
        return self

# Store original __import__
orig_import = __import__


def import_mock(name, *args, **kwargs):
    try:
        imports = orig_import(name, *args, **kwargs)
    except ImportError:
        # Create a mock module for missing imports with better type handling
        imports = mock.MagicMock()
        # Set common attributes to avoid warnings
        imports.__name__ = name
        imports.__file__ = f"<mock:{name}>"
        imports.__spec__ = mock.MagicMock()
        imports.__spec__.name = name

    # Handle fromlist from args or kwargs
    fromlist = kwargs.get('fromlist', args[2] if len(args) > 2 else [])

    # Enhanced mantid.api mocking
    if name == "mantid.api" and fromlist:
        if "PythonAlgorithm" in fromlist:
            imports.PythonAlgorithm = MockPythonAlgorithm
        # Mock AlgorithmFactory to prevent registration errors
        if "AlgorithmFactory" in fromlist:
            mock_factory = mock.MagicMock()
            mock_factory.register = mock.MagicMock()
            imports.AlgorithmFactory = mock_factory

    # Enhanced mantid.dataobjects mocking
    if name == "mantid.dataobjects" or (name == "mantid" and "dataobjects" in fromlist):
        # Create comprehensive dataobjects mock
        dataobjects_mock = mock.MagicMock()
        dataobjects_mock.__name__ = "mantid.dataobjects"
        dataobjects_mock.__file__ = "<mock:mantid.dataobjects>"
        if name == "mantid.dataobjects":
            imports = dataobjects_mock
        else:
            imports.dataobjects = dataobjects_mock

    # Enhanced mantid.simpleapi mocking
    if name == "mantid.simpleapi" or (name == "mantid" and "simpleapi" in fromlist):
        simpleapi_mock = mock.MagicMock()
        simpleapi_mock.__name__ = "mantid.simpleapi"
        simpleapi_mock.__file__ = "<mock:mantid.simpleapi>"
        if name == "mantid.simpleapi":
            imports = simpleapi_mock
        else:
            imports.simpleapi = simpleapi_mock

    # Enhanced mantid.kernel mocking
    if name == "mantid.kernel" or (name == "mantid" and "kernel" in fromlist):
        kernel_mock = mock.MagicMock()
        kernel_mock.__name__ = "mantid.kernel"
        kernel_mock.__file__ = "<mock:mantid.kernel>"
        if name == "mantid.kernel":
            imports = kernel_mock
        else:
            imports.kernel = kernel_mock

    # Enhanced mantid.plots mocking
    if name == "mantid.plots" or (name == "mantid" and "plots" in fromlist):
        plots_mock = mock.MagicMock()
        plots_mock.__name__ = "mantid.plots"
        plots_mock.__file__ = "<mock:mantid.plots>"
        if name == "mantid.plots":
            imports = plots_mock
        else:
            imports.plots = plots_mock

    if name == "qtpy.QtCore" and fromlist:
        if "QThread" in fromlist:
            imports.QThread = MockQThread
        if "Signal" in fromlist:
            imports.Signal = MockSignal
        if "Property" in fromlist:
            imports.Property = MockProperty
    if name == "mantid.geometry" and fromlist:
        if "CrystalStructure" in fromlist:
            imports.CrystalStructure = MockCrystalStructure

    # Handle common type imports that cause pydantic warnings
    if hasattr(imports, 'bool_') and not callable(imports.bool_):
        imports.bool_ = MockBool

    return imports


with mock.patch("builtins.__import__", side_effect=import_mock):
    try:
        from mantid.api import PythonAlgorithm
        from qtpy.QtCore import Property, QThread, Signal
    except ImportError:
        # Use our mock classes if imports fail
        PythonAlgorithm = MockPythonAlgorithm
        Property = MockProperty
        QThread = MockQThread
        Signal = MockSignal

    # Mock AlgorithmFactory globally to prevent registration errors
    import sys
    mock_algorithm_factory = mock.MagicMock()
    mock_algorithm_factory.register = mock.MagicMock()

    # Create comprehensive mock modules to prevent import errors
    mock_modules = {
        'mantid': mock.MagicMock(),
        'mantid.api': mock.MagicMock(),
        'mantid.dataobjects': mock.MagicMock(),
        'mantid.simpleapi': mock.MagicMock(),
        'mantid.kernel': mock.MagicMock(),
        'mantid.utils': mock.MagicMock(),
        'mantid.utils.logging': mock.MagicMock(),
        'mantid.geometry': mock.MagicMock(),
        'mantid.plots': mock.MagicMock(),
        'mantid.plots.plotfunctions': mock.MagicMock(),
        'mantid.plots.datafunctions': mock.MagicMock(),
        'mantid.plots.utility': mock.MagicMock(),
        'mantidqt': mock.MagicMock(),
        'mantidqt.widgets': mock.MagicMock(),
        'mantidqt.widgets.algorithmprogress': mock.MagicMock(),
        'qtpy': mock.MagicMock(),
        'qtpy.uic': mock.MagicMock(),
        'qtpy.QtWidgets': mock.MagicMock(),
        'qtpy.QtGui': mock.MagicMock(),
        'qtpy.QtCore': mock.MagicMock(),
        'workbench': mock.MagicMock(),
        'workbench.plotting': mock.MagicMock(),
        'workbench.plugins': mock.MagicMock(),
        'workbench.plugins.workspacewidget': mock.MagicMock(),
        'snapred.backend.data': mock.MagicMock(),
        'snapred.backend.service': mock.MagicMock(),
        'snapred.backend.recipe.algorithm': mock.MagicMock(),
        'snapred.ui.handler': mock.MagicMock(),
        'snapred.ui.presenter': mock.MagicMock(),
        'snapred.ui.workflow': mock.MagicMock(),
        'snapred.ui.widget': mock.MagicMock(),
        'snapred.ui.view': mock.MagicMock(),
        'snapred.ui.view.reduction': mock.MagicMock(),
        'snapred.ui.main': mock.MagicMock(),
        'snapred.ui.threading': mock.MagicMock(),
        'snapred.backend.recipe': mock.MagicMock(),
        'snapred.backend.api': mock.MagicMock(),
    }

    # Set proper module attributes
    for module_name, module_mock in mock_modules.items():
        module_mock.__name__ = module_name
        module_mock.__file__ = f"<mock:{module_name}>"
        module_mock.__spec__ = mock.MagicMock()
        module_mock.__spec__.name = module_name
        sys.modules[module_name] = module_mock

    # Set up specific mocks
    sys.modules['mantid.api'].AlgorithmFactory = mock_algorithm_factory
    sys.modules['mantid.api'].PythonAlgorithm = MockPythonAlgorithm
    sys.modules['qtpy.QtCore'].Signal = MockSignal
    sys.modules['qtpy.QtCore'].Property = MockProperty
    sys.modules['qtpy.QtCore'].QThread = MockQThread

    # Create mock classes for common missing attributes
    def create_mock_class(name):
        mock_class = mock.MagicMock()
        mock_class.__name__ = name
        mock_class.__module__ = f"mock.{name}"
        return mock_class

    sys.modules['snapred.backend.data'].DataExportService = create_mock_class('DataExportService')
    sys.modules['snapred.backend.data'].DataFactoryService = create_mock_class('DataFactoryService')
    sys.modules['snapred.backend.data'].LocalDataService = create_mock_class('LocalDataService')
    sys.modules['snapred.backend.data'].GroceryService = create_mock_class('GroceryService')
    sys.modules['snapred.backend.service'].CalibrantSampleService = create_mock_class('CalibrantSampleService')
    sys.modules['snapred.backend.service'].CalibrationService = create_mock_class('CalibrationService')
    sys.modules['snapred.backend.service'].ConfigLookupService = create_mock_class('ConfigLookupService')
    sys.modules['snapred.backend.service'].CrystallographicInfoService = create_mock_class('CrystallographicInfoService')
    sys.modules['snapred.backend.service'].LiteDataService = create_mock_class('LiteDataService')
    sys.modules['snapred.backend.service'].NormalizationService = create_mock_class('NormalizationService')
    sys.modules['snapred.backend.service'].ServiceFactory = create_mock_class('ServiceFactory')
    sys.modules['snapred.backend.service'].StateIdLookupService = create_mock_class('StateIdLookupService')
    sys.modules['snapred.backend.service'].WorkspaceService = create_mock_class('WorkspaceService')
    sys.modules['snapred.ui.threading'].worker_pool = create_mock_class('worker_pool')

    autodoc_mock_imports = [
        "mantid",
        "mantid.api",
        "mantid.api.AlgorithmFactory",
        "mantid.kernel",
        "mantid.utils",
        "mantid.utils.logging",
        "mantid.simpleapi",
        "mantid.geometry",
        "mantid.dataobjects",
        "mantidqt",
        "mantidqt.widgets",
        "mantidqt.widgets.algorithmprogress",
        "qtpy",
        "qtpy.uic",
        "qtpy.QtWidgets",
        "qtpy.QtGui",
        "qtpy.QtCore",
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

    # Suppress certain warnings that are expected in documentation builds
    suppress_warnings = [
        'autodoc.import_object',  # Suppress autodoc import warnings
    ]

    # Configure autodoc to be more lenient
    autodoc_default_options = {
        'members': True,
        'undoc-members': True,
        'show-inheritance': True,
        'ignore-module-all': True,
    }

    # Suppress pydantic warnings during documentation builds
    import warnings
    warnings.filterwarnings('ignore', category=UserWarning, module='pydantic')

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

        try:
            erd.draw(clazz, out=clazzPath)
        except Exception as e:
            # Handle cases where graphviz is not available or other erdantic issues
            print(f"Warning: Could not generate diagram for {clazz.__name__}: {e}")
            # Create a placeholder SVG file
            with open(clazzPath, "w") as f:
                f.write(f"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 100">
                    <rect width="200" height="100" fill="#f0f0f0" stroke="#ccc"/>
                    <text x="100" y="50" text-anchor="middle" font-family="Arial" font-size="12">
                        {clazz.__name__} Diagram
                    </text>
                    <text x="100" y="70" text-anchor="middle" font-family="Arial" font-size="10">
                        (Diagram generation unavailable)
                    </text>
                </svg>""")
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
