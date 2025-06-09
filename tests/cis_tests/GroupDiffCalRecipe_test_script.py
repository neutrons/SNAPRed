from collections.abc import Sequence
import logging
from pathlib import Path
import sys

from mantid.api import IEventWorkspace, MatrixWorkspace
from mantid.dataobjects import GroupingWorkspace
from mantid.kernel import ConfigService

# Fake-out SNAPRed's `Config` initialization so it will use the unit-test environment:
sys.modules['conftest'] = "DUMMY_CONFTEST"

import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent
import snapred.backend.recipe.algorithm.CalculateDiffCalTable
from mantid.simpleapi import *
from mantid.utils.logging import log_to_python

from snapred.meta.Config import Config, Resource, datasearch_directories, fromMantidLoggingLevel, fromPythonLoggingLevel

# the algorithm to test


"""
from snapred.backend.recipe.GroupDiffCalRecipe import (
    GroupDiffCalRecipe as Recipe,  # noqa: E402
)
"""
from snapred.backend.recipe.GroupDiffCalRecipe_old import (
    GroupDiffCalRecipe as Recipe,  # noqa: E402
)


sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import deleteWorkspaceNoThrow, maskGroups, mutableWorkspaceClones
from util.script_as_test import not_a_test

##
## Create "by hand" test code from the actual unit-test code at `test_GroupDiffCalRecipe.py`:
##
logger = logging.getLogger("TEST")
logger.setLevel(logging.DEBUG)

# "log_to_python" seems to turn off all useful low-level logging!  :(
# log_to_python(level=fromPythonLoggingLevel(logging.DEBUG))
ConfigService.setLogLevel(fromPythonLoggingLevel(logging.DEBUG))

@not_a_test
class TestGroupDiffCalRecipe():
    
    @classmethod
    def setUpClass(cls):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        syntheticInputs = SyntheticData()
        cls.fakeIngredients = syntheticInputs.ingredients
        
        # *** DEBUG ***
        # cls.fakeIngredients.maxChiSq = 1000.0
        
        fakeDBin = max([abs(d) for d in cls.fakeIngredients.pixelGroup.dBin()])

        runNumber = cls.fakeIngredients.runConfig.runNumber
        cls.fakeRawData = f"_test_groupcal_{runNumber}"
        cls.fakeGroupingWorkspace = f"_test_groupcal_difc_{runNumber}"
        cls.fakeMaskWorkspace = f"_test_groupcal_difc_{runNumber}_mask"
        cls.difcWS = f"_{runNumber}_difcs_test"
        syntheticInputs.generateWorkspaces(cls.fakeRawData, cls.fakeGroupingWorkspace, cls.fakeMaskWorkspace)

        # create the DIFCprev table
        CalculateDiffCalTable(
            InputWorkspace=cls.fakeRawData,
            CalibrationTable=cls.difcWS,
            OffsetMode="Signed",
            BinWidth=fakeDBin,
        )

        # log_to_python(level=fromPythonLoggingLevel(logging.DEBUG))
        mantidLogger = logging.getLogger("Mantid")
        mantidLogger.setLevel(logging.DEBUG)
        
        # the stream handler will print alongside the SNAPRed logs
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG)


    @classmethod
    def tearDownClass(cls) -> None:
        """
        Delete all workspaces created by this test, and remove any created files.
        This is run once at the end of this test suite.
        """
        for ws in [cls.fakeRawData, cls.fakeGroupingWorkspace, cls.fakeMaskWorkspace, cls.difcWS]:
            deleteWorkspaceNoThrow(ws)
        # return super().tearDownClass()

    def test_execute(self):
        """Test that the recipe executes"""

        uniquePrefix = "test_e_"
        (maskWS,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix)
        (maskWSName,) = mutableWorkspaceClones((self.fakeMaskWorkspace,), uniquePrefix, name_only=True)

        groceries = {
            "inputWorkspace": self.fakeRawData,
            "groupingWorkspace": self.fakeGroupingWorkspace,
            "calibrationTable": "_final_DIFC_table",
            "maskWorkspace": maskWSName,
            "outputWorkspace": f"_test_out_dsp_{self.fakeIngredients.runConfig.runNumber}",
            "previousCalibration": self.difcWS,
        }
        res = Recipe().cook(self.fakeIngredients, groceries)

        # assert res.result
        # assert maskWSName in mtd
        # assert maskWS.getNumberMasked() == 0
        print(f"RESULT: {res.result}")
        print(f"mask in mtd: {maskWSName in mtd}")
        print(f"number masked: {maskWS.getNumberMasked()}")
        
        deleteWorkspaceNoThrow(maskWSName)


# Actually run the failing test:
TestGroupDiffCalRecipe.setUpClass()
test = TestGroupDiffCalRecipe()
test.test_execute()
# TestGroupDiffCalRecipe.tearDownClass()


