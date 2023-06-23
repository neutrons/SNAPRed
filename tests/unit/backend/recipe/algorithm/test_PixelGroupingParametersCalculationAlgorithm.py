import json
import socket
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
        # "mantid.api": mock.Mock(),
        # "mantid.kernel": mock.Mock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from mantid.simpleapi import DeleteWorkspace, mtd
    from pydantic import parse_file_as
    from snapred.backend.dao.calibration.Calibration import Calibration
    from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
    from snapred.backend.recipe.algorithm.PixelGroupingParametersCalculationAlgorithm import (
        PixelGroupingParametersCalculationAlgorithm,
    )

    IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

    def setup():
        """Common setup before each test"""
        pass

    def teardown():
        """Common teardown after each test"""
        if not IS_ON_ANALYSIS_MACHINE:  # noqa: F821
            return
        # collect list of all workspaces
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for workspace in workspaces:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")

    @pytest.fixture(autouse=True)
    def _setup_teardown():
        """Setup before each test, teardown after each test"""
        setup()
        yield
        teardown()

    class TestPixelGroupingParametersCalculation:
        def getCalibrationState(self):
            if not IS_ON_ANALYSIS_MACHINE:  # noqa: F821
                return ""
            else:
                return parse_file_as(
                    Calibration, "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/CalibrationParameters.json"
                ).json()

        def test_column(self):
            instrumentDefinitionFile = "/opt/anaconda/envs/mantid-dev/instrument/SNAP_Definition_2011-09-07.xml"
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.xml"
            referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_parameters.json"

            self.run_test(
                instrumentDefinitionFile=instrumentDefinitionFile,
                calibrationState=self.getCalibrationState(),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
                reverseGroupingIndex=False,
            )

        def test_column_lite(self):
            instrumentDefinitionFile = "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
            referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_lite_parameters.json"

            self.run_test(
                instrumentDefinitionFile=instrumentDefinitionFile,
                calibrationState=self.getCalibrationState(),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
                reverseGroupingIndex=True,
            )

        def test_bank(self):
            instrumentDefinitionFile = "/opt/anaconda/envs/mantid-dev/instrument/SNAP_Definition_2011-09-07.xml"
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Bank.xml"
            referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Bank_parameters.json"

            self.run_test(
                instrumentDefinitionFile=instrumentDefinitionFile,
                calibrationState=self.getCalibrationState(),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
                reverseGroupingIndex=False,
            )

        def test_bank_lite(self):
            instrumentDefinitionFile = "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Bank.lite.nxs"
            referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Bank_lite_parameters.json"

            self.run_test(
                instrumentDefinitionFile=instrumentDefinitionFile,
                calibrationState=self.getCalibrationState(),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
                reverseGroupingIndex=True,
            )

        def test_all(self):
            instrumentDefinitionFile = "/opt/anaconda/envs/mantid-dev/instrument/SNAP_Definition_2011-09-07.xml"
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_All.xml"
            referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/All_parameters.json"

            self.run_test(
                instrumentDefinitionFile=instrumentDefinitionFile,
                calibrationState=self.getCalibrationState(),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
                reverseGroupingIndex=False,
            )

        def test_all_lite(self):
            instrumentDefinitionFile = "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_All.lite.nxs"
            referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/All_lite_parameters.json"

            self.run_test(
                instrumentDefinitionFile=instrumentDefinitionFile,
                calibrationState=self.getCalibrationState(),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
                reverseGroupingIndex=True,
            )

        def test_wrong_idf(self):
            instrumentDefinitionFile = "junk"
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
            referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_lite_parameters.json"
            with pytest.raises(RuntimeError) as excinfo:
                self.run_test(
                    instrumentDefinitionFile=instrumentDefinitionFile,
                    calibrationState=self.getCalibrationState(),
                    groupingFile=groupingFile,
                    referenceParametersFile=referenceParametersFile,
                    reverseGroupingIndex=True,
                )
            assert "FileDescriptor" in str(excinfo.value)

        def test_wrong_grouping_file(self):
            instrumentDefinitionFile = "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
            groupingFile = "junk"
            referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_lite_parameters.json"
            with pytest.raises(RuntimeError) as excinfo:
                self.run_test(
                    instrumentDefinitionFile=instrumentDefinitionFile,
                    calibrationState=self.getCalibrationState(),
                    groupingFile=groupingFile,
                    referenceParametersFile=referenceParametersFile,
                    reverseGroupingIndex=True,
                )
            assert "Filename" in str(excinfo.value)

        def test_wrong_calibration_state(self):
            instrumentDefinitionFile = "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
            groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
            referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_lite_parameters.json"
            with pytest.raises(RuntimeError) as excinfo:
                self.run_test(
                    instrumentDefinitionFile=instrumentDefinitionFile,
                    calibrationState="junk",
                    groupingFile=groupingFile,
                    referenceParametersFile=referenceParametersFile,
                    reverseGroupingIndex=True,
                )
            assert "Calibration" in str(excinfo.value)

        def run_test(
            self,
            instrumentDefinitionFile,
            calibrationState,
            groupingFile,
            referenceParametersFile,
            reverseGroupingIndex,
        ):
            """Test execution of PixelGroupingParametersCalculationAlgorithm"""

            if not IS_ON_ANALYSIS_MACHINE:  # noqa: F821
                return

            pixelGroupingAlgo = PixelGroupingParametersCalculationAlgorithm()
            pixelGroupingAlgo.initialize()

            pixelGroupingAlgo.setProperty("InputState", calibrationState)
            pixelGroupingAlgo.setProperty("InstrumentDefinitionFile", instrumentDefinitionFile)
            pixelGroupingAlgo.setProperty("GroupingFile", groupingFile)

            assert pixelGroupingAlgo.execute()

            # parse the algorithm output and create a list of PixelGroupingParameters
            pixelGroupingParams_str = json.loads(pixelGroupingAlgo.getProperty("OutputParameters").value)
            pixelGroupingParams_calc = []
            for item in pixelGroupingParams_str:
                pixelGroupingParams_calc.append(PixelGroupingParameters.parse_raw(item))

            # parse the reference file. Note, in the reference file each kind of parameter is grouped into its own list
            f = open(referenceParametersFile)
            pixelGroupingParams_ref = json.load(f)

            # compare calculated and reference parameters
            number_of_groupings_calc = len(pixelGroupingParams_calc)
            assert len(pixelGroupingParams_ref["twoTheta"]) == number_of_groupings_calc
            assert len(pixelGroupingParams_ref["dMin"]) == number_of_groupings_calc
            assert len(pixelGroupingParams_ref["dMax"]) == number_of_groupings_calc
            assert len(pixelGroupingParams_ref["delDOverD"]) == number_of_groupings_calc

            # reverseGroupingIndex takes care of the different order of pixel groupings between "full" anf "lite"
            # instruments. This has to do with how Mantid treats different kinds of grouping files used by
            # PixelGroupingParametersCalculationAlgorithm
            index = 0 if reverseGroupingIndex else number_of_groupings_calc - 1
            for param in pixelGroupingParams_ref["twoTheta"]:
                assert abs(float(param) - pixelGroupingParams_calc[index].twoTheta) < 1.0e-5
                index += 1 if reverseGroupingIndex else -1

            index = 0 if reverseGroupingIndex else number_of_groupings_calc - 1
            for param in pixelGroupingParams_ref["dMin"]:
                assert abs(float(param) - pixelGroupingParams_calc[index].dResolution.minimum) < 1.0e-4
                index += 1 if reverseGroupingIndex else -1

            index = 0 if reverseGroupingIndex else number_of_groupings_calc - 1
            for param in pixelGroupingParams_ref["dMax"]:
                assert abs(float(param) - pixelGroupingParams_calc[index].dResolution.maximum) < 1.0e-4
                index += 1 if reverseGroupingIndex else -1

            index = 0 if reverseGroupingIndex else number_of_groupings_calc - 1
            for param in pixelGroupingParams_ref["delDOverD"]:
                assert abs(float(param) - pixelGroupingParams_calc[index].dRelativeResolution) < 1.0e-3
                index += 1 if reverseGroupingIndex else -1
