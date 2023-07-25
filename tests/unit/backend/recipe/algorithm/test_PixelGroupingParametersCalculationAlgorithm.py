import json
import socket
import unittest.mock as mock

import pytest

with mock.patch.dict(
    "sys.modules",
    {
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

    def getInstrumentState():
        return parse_file_as(
            Calibration, "/SNS/SNAP/shared/Calibration_Prototype/Powder/04bd2c53f6bf6754/CalibrationParameters.json"
        ).instrumentState.json()

    def getInstrumentDefinitionFilePath(isLite=True):
        if isLite:
            return "/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml"
        else:
            return "/opt/anaconda/envs/mantid-dev/instrument/SNAP_Definition.xml"

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_column():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.xml"
        referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_parameters_newCalc.json"

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(isLite=False),
            instrumentState=getInstrumentState(),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_column_lite():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_parameters_newCalc.lite.json"
        )

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(),
            instrumentState=getInstrumentState(),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_bank():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Bank.xml"
        referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Bank_parameters_newCalc.json"

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(isLite=False),
            instrumentState=getInstrumentState(),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_bank_lite():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Bank.lite.nxs"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Bank_parameters_newCalc.lite.json"
        )

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(),
            instrumentState=getInstrumentState(),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_all():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_All.xml"
        referenceParametersFile = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/All_parameters_newCalc.json"

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(isLite=False),
            instrumentState=getInstrumentState(),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_all_lite():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_All.lite.nxs"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/All_parameters_newCalc.lite.json"
        )

        run_test(
            instrumentDefinitionFile=getInstrumentDefinitionFilePath(),
            instrumentState=getInstrumentState(),
            groupingFile=groupingFile,
            referenceParametersFile=referenceParametersFile,
        )

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_wrong_idf():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_parameters_newCalc.lite.json"
        )
        with pytest.raises(RuntimeError) as excinfo:
            run_test(
                instrumentDefinitionFile="junk",
                instrumentState=getInstrumentState(),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
            )
        assert "FileDescriptor" in str(excinfo.value)

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_wrong_grouping_file():
        groupingFile = "junk"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_parameters_newCalc.lite.json"
        )

        with pytest.raises(RuntimeError) as excinfo:
            run_test(
                instrumentDefinitionFile=getInstrumentDefinitionFilePath(),
                instrumentState=getInstrumentState(),
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
            )
        assert "Filename" in str(excinfo.value)

    @pytest.mark.skipif(not IS_ON_ANALYSIS_MACHINE, reason="requires analysis datafiles")
    def test_wrong_instrument_state():
        groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.lite.nxs"
        referenceParametersFile = (
            "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/Column_parameters_newCalc.lite.json"
        )

        with pytest.raises(RuntimeError) as excinfo:
            run_test(
                instrumentDefinitionFile=getInstrumentDefinitionFilePath(),
                instrumentState="junk",
                groupingFile=groupingFile,
                referenceParametersFile=referenceParametersFile,
            )
        assert "InstrumentState" in str(excinfo.value)

    def run_test(instrumentDefinitionFile, instrumentState, groupingFile, referenceParametersFile):
        """Test execution of PixelGroupingParametersCalculationAlgorithm"""
        pixelGroupingAlgo = PixelGroupingParametersCalculationAlgorithm()
        pixelGroupingAlgo.initialize()

        pixelGroupingAlgo.setProperty("InstrumentState", instrumentState)
        pixelGroupingAlgo.setProperty("InstrumentDefinitionFile", instrumentDefinitionFile)
        pixelGroupingAlgo.setProperty("GroupingFile", groupingFile)

        assert pixelGroupingAlgo.execute()

        # parse the algorithm output and create a list of PixelGroupingParameters
        pixelGroupingParams_json = json.loads(pixelGroupingAlgo.getProperty("OutputParameters").value)
        pixelGroupingParams_calc = []
        for item in pixelGroupingParams_json:
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

        index = 0
        for param in pixelGroupingParams_ref["twoTheta"]:
            assert abs(float(param) - pixelGroupingParams_calc[index].twoTheta) == 0
            index += 1

        index = 0
        for param in pixelGroupingParams_ref["dMin"]:
            assert abs(float(param) - pixelGroupingParams_calc[index].dResolution.minimum) == 0
            index += 1

        index = 0
        for param in pixelGroupingParams_ref["dMax"]:
            assert abs(float(param) - pixelGroupingParams_calc[index].dResolution.maximum) == 0
            index += 1

        index = 0
        for param in pixelGroupingParams_ref["delDOverD"]:
            assert abs(float(param) - pixelGroupingParams_calc[index].dRelativeResolution) < 1.0e-3
            index += 1
