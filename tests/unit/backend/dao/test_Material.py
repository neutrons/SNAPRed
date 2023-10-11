import unittest

from mantid.simpleapi import CreateWorkspace, SetSample
from snapred.backend.dao.state.CalibrantSample.Material import Material


class TestGeometry(unittest.TestCase):
    def setUp(self):
        self.vanadium = Material(
            packingFraction=0.3,
            chemicalFormula="V",
        )
        self.vanadiumBoron = Material(
            packingFraction=0.3,
            massDensity=0.3,
            chemicalFormula="V B",
        )

    def test_singleElementMaterial(self):
        # ensure that the materialDictionary object
        # returns correct dictionary for single element
        ref = {
            "ChemicalFormula": self.vanadium.chemicalFormula,
            "PackingFraction": self.vanadium.packingFraction,
        }
        assert self.vanadium.materialDictionary == ref

    def test_twoElementMaterial(self):
        # ensure that the materialDictionary object
        # returns correct dictionary for two elements
        ref = {
            "ChemicalFormula": self.vanadiumBoron.chemicalFormula,
            "MassDensity": self.vanadiumBoron.massDensity,
            "PackingFraction": self.vanadiumBoron.packingFraction,
        }
        assert self.vanadiumBoron.materialDictionary == ref

    def test_settableInMantid(self):
        # test that these can be used to set the sample in mantid
        # run SetSample with the dictionary to set shape
        # then get output XML of the sample shape
        # note that the XML converts from cm to m
        sampleWS = CreateWorkspace(
            DataX=1,
            DataY=1,
        )
        # test setting with a single-element crystal
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadium.materialDictionary,
        )
        material = sampleWS.sample().getMaterial()
        print(dir(material.chemicalFormula()[0][0]))
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.packingFraction == self.vanadium.packingFraction

        # test setting with a mutli-element crystal
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumBoron.materialDictionary,
        )
        material = sampleWS.sample().getMaterial()
        print(dir(material.chemicalFormula()[0][0]))
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.chemicalFormula()[0][1].symbol == "B"
        assert material.packingFraction == self.vanadiumBoron.packingFraction
