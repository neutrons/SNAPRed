import unittest

# mantid imports
from mantid.simpleapi import CreateWorkspace, NormalizeByCurrentButTheCorrectWay, mtd


class TestNormalizeByCurrent(unittest.TestCase):
    # @mock.patch("snaped.backend.recipe.algorithm.NormalizeByCurrentButTheCorrectWay.NormaliseByCurrent")
    def test_normalize_unnormalized(self):
        value = 16
        protoncharge = 2
        wsname = mtd.unique_name()
        ws = CreateWorkspace(OutputWorkspace=wsname, DataX=[1], DataY=[value])
        ws.mutableRun().addProperty("gd_prtn_chrg", float(protoncharge), True)
        assert not ws.run().hasProperty("NormalizationFactor")
        NormalizeByCurrentButTheCorrectWay(
            InputWorkspace=wsname,
            OutputWorkspace=wsname,
        )
        assert ws.run().hasProperty("NormalizationFactor")
        assert ws.run().getProperty("NormalizationFactor").value == protoncharge
        assert ws.dataY(0) == [value / protoncharge]

    def test_normalize_already_normalized(self):
        value = 16
        protoncharge = 2
        wsname = mtd.unique_name()
        ws = CreateWorkspace(OutputWorkspace=wsname, DataX=[1], DataY=[value])
        ws.mutableRun().addProperty("gd_prtn_chrg", protoncharge, True)
        ws.mutableRun().addProperty("NormalizationFactor", protoncharge, True)
        NormalizeByCurrentButTheCorrectWay(
            InputWorkspace=wsname,
            OutputWorkspace=wsname,
        )
        assert ws.run().hasProperty("NormalizationFactor")
        print(ws.run().getProperty("NormalizationFactor").value)
        assert ws.dataY(0) == [value]
